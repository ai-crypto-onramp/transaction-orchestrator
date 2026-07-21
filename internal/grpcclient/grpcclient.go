// Package grpcclient provides gRPC client adapters for the six saga partner
// services.  Each adapter wraps the generated pb client and implements the
// corresponding interface from internal/partner, translating between the
// protobuf request/response types and the plain Go structs used by the saga
// steps.
//
// Denial / transient errors are mapped to partner.ErrDenied and
// partner.ErrTransient respectively via gRPC status codes:
//   - codes.PermissionDenied / FailedPrecondition -> ErrDenied
//   - codes.Unavailable / DeadlineExceeded        -> ErrTransient
package grpcclient

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"time"

	"github.com/ai-crypto-onramp/transaction-orchestrator/internal/partner"
	pbblockchain "github.com/ai-crypto-onramp/transaction-orchestrator/internal/pb/blockchain"
	pbkyt "github.com/ai-crypto-onramp/transaction-orchestrator/internal/pb/kyt"
	pbledger "github.com/ai-crypto-onramp/transaction-orchestrator/internal/pb/ledger"
	pbmpc "github.com/ai-crypto-onramp/transaction-orchestrator/internal/pb/mpc"
	pbpayment "github.com/ai-crypto-onramp/transaction-orchestrator/internal/pb/payment"
	pbpolicy "github.com/ai-crypto-onramp/transaction-orchestrator/internal/pb/policy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// defaultCallTimeout caps each individual RPC.
const defaultCallTimeout = 30 * time.Second

// loadTLSConfig reads TLS_CERT_FILE / TLS_KEY_FILE / TLS_CA_FILE and returns
// a *tls.Config suitable for grpc.WithTransportCredentials(credentials.NewTLS).
// In DEV_MODE=1 with all three unset it returns nil (caller falls back to
// insecure). In prod a missing trio is fatal.
func loadTLSConfig() (*tls.Config, error) {
	cert := os.Getenv("TLS_CERT_FILE")
	key := os.Getenv("TLS_KEY_FILE")
	ca := os.Getenv("TLS_CA_FILE")
	if cert == "" && key == "" && ca == "" {
		if os.Getenv("DEV_MODE") == "1" {
			return nil, nil
		}
		return nil, fmt.Errorf("grpcclient: TLS_CERT_FILE/TLS_KEY_FILE/TLS_CA_FILE required when DEV_MODE!=1")
	}
	if cert == "" || key == "" || ca == "" {
		return nil, fmt.Errorf("grpcclient: TLS_CERT_FILE, TLS_KEY_FILE and TLS_CA_FILE must all be set together")
	}
	pair, err := tls.LoadX509KeyPair(cert, key)
	if err != nil {
		return nil, fmt.Errorf("grpcclient: load keypair: %w", err)
	}
	caPEM, err := os.ReadFile(ca)
	if err != nil {
		return nil, fmt.Errorf("grpcclient: read CA: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("grpcclient: failed to parse CA bundle %s", ca)
	}
	return &tls.Config{
		Certificates: []tls.Certificate{pair},
		RootCAs:      pool,
		MinVersion:   tls.VersionTLS12,
	}, nil
}

// transportCredentials returns the gRPC dial credentials for partner dials:
// TLS when configured, insecure in DEV_MODE when no TLS material is present.
func transportCredentials() (credentials.TransportCredentials, error) {
	cfg, err := loadTLSConfig()
	if err != nil {
		return nil, err
	}
	if cfg == nil {
		return insecure.NewCredentials(), nil
	}
	return credentials.NewTLS(cfg), nil
}

// --- status -> partner error ----------------------------------------------

func mapErr(err error) error {
	if err == nil {
		return nil
	}
	st, ok := status.FromError(err)
	if !ok {
		return err
	}
	switch st.Code() {
	case codes.PermissionDenied, codes.FailedPrecondition:
		return fmt.Errorf("%w: %s", partner.ErrDenied, st.Message())
	case codes.Unavailable, codes.DeadlineExceeded:
		return fmt.Errorf("%w: %s", partner.ErrTransient, st.Message())
	default:
		return err
	}
}

// --- policy ------------------------------------------------------------------

// PolicyClient wraps the generated gRPC client and implements partner.Policy.
type PolicyClient struct {
	cc pbpolicy.PolicyRiskEngineClient
}

// NewPolicy dials target and returns a PolicyClient.
func NewPolicy(ctx context.Context, target string) (*PolicyClient, *grpc.ClientConn, error) {
	creds, err := transportCredentials()
	if err != nil {
		return nil, nil, fmt.Errorf("policy tls: %w", err)
	}
	cc, err := grpc.NewClient(target, grpc.WithTransportCredentials(creds))
	if err != nil {
		return nil, nil, fmt.Errorf("policy dial %s: %w", target, err)
	}
	return &PolicyClient{cc: pbpolicy.NewPolicyRiskEngineClient(cc)}, cc, nil
}

// Evaluate calls PolicyRiskEngine.Evaluate.
func (c *PolicyClient) Evaluate(ctx context.Context, req partner.PolicyRequest) (partner.PolicyResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, defaultCallTimeout)
	defer cancel()
	resp, err := c.cc.Evaluate(ctx, &pbpolicy.PolicyRequest{
		TxId: req.TxID, UserId: req.UserID, QuoteId: req.QuoteID,
		Amount: req.Amount, Asset: req.Asset, Rail: req.Rail, DestAddress: req.DestAddress,
	})
	if err != nil {
		return partner.PolicyResponse{}, mapErr(err)
	}
	return partner.PolicyResponse{Decision: partner.PolicyDecision(resp.Decision), Reason: resp.Reason}, nil
}

// --- payment -----------------------------------------------------------------

// PaymentClient wraps the generated gRPC client and implements partner.Payment.
type PaymentClient struct {
	cc pbpayment.PaymentOrchestrationClient
}

// NewPayment dials target and returns a PaymentClient.
func NewPayment(ctx context.Context, target string) (*PaymentClient, *grpc.ClientConn, error) {
	creds, err := transportCredentials()
	if err != nil {
		return nil, nil, fmt.Errorf("payment tls: %w", err)
	}
	cc, err := grpc.NewClient(target, grpc.WithTransportCredentials(creds))
	if err != nil {
		return nil, nil, fmt.Errorf("payment dial %s: %w", target, err)
	}
	return &PaymentClient{cc: pbpayment.NewPaymentOrchestrationClient(cc)}, cc, nil
}

// Authorize calls PaymentOrchestration.Authorize.
func (c *PaymentClient) Authorize(ctx context.Context, req partner.PaymentAuthorizeRequest) (partner.PaymentAuthorizeResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, defaultCallTimeout)
	defer cancel()
	resp, err := c.cc.Authorize(ctx, &pbpayment.PaymentAuthorizeRequest{
		TxId: req.TxID, UserId: req.UserID, QuoteId: req.QuoteID,
		Amount: req.Amount, Asset: req.Asset, Rail: req.Rail,
	})
	if err != nil {
		return partner.PaymentAuthorizeResponse{}, mapErr(err)
	}
	return partner.PaymentAuthorizeResponse{AuthID: resp.AuthId}, nil
}

// Capture calls PaymentOrchestration.Capture.
func (c *PaymentClient) Capture(ctx context.Context, req partner.PaymentCaptureRequest) (partner.PaymentCaptureResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, defaultCallTimeout)
	defer cancel()
	resp, err := c.cc.Capture(ctx, &pbpayment.PaymentCaptureRequest{
		TxId: req.TxID, AuthId: req.AuthID, Amount: req.Amount, Asset: req.Asset,
	})
	if err != nil {
		return partner.PaymentCaptureResponse{}, mapErr(err)
	}
	return partner.PaymentCaptureResponse{CaptureID: resp.CaptureId}, nil
}

// VoidAuthorization calls PaymentOrchestration.VoidAuthorization.
func (c *PaymentClient) VoidAuthorization(ctx context.Context, req partner.PaymentVoidRequest) error {
	ctx, cancel := context.WithTimeout(ctx, defaultCallTimeout)
	defer cancel()
	_, err := c.cc.VoidAuthorization(ctx, &pbpayment.PaymentVoidRequest{TxId: req.TxID, AuthId: req.AuthID})
	return mapErr(err)
}

// Refund calls PaymentOrchestration.Refund.
func (c *PaymentClient) Refund(ctx context.Context, req partner.PaymentRefundRequest) error {
	ctx, cancel := context.WithTimeout(ctx, defaultCallTimeout)
	defer cancel()
	_, err := c.cc.Refund(ctx, &pbpayment.PaymentRefundRequest{
		TxId: req.TxID, CaptureId: req.CaptureID, Amount: req.Amount, Asset: req.Asset,
	})
	return mapErr(err)
}

// --- kyt ---------------------------------------------------------------------

// KytClient wraps the generated gRPC client and implements partner.Kyt.
type KytClient struct{ cc pbkyt.AmlKytScreeningClient }

// NewKyt dials target and returns a KytClient.
func NewKyt(ctx context.Context, target string) (*KytClient, *grpc.ClientConn, error) {
	creds, err := transportCredentials()
	if err != nil {
		return nil, nil, fmt.Errorf("kyt tls: %w", err)
	}
	cc, err := grpc.NewClient(target, grpc.WithTransportCredentials(creds))
	if err != nil {
		return nil, nil, fmt.Errorf("kyt dial %s: %w", target, err)
	}
	return &KytClient{cc: pbkyt.NewAmlKytScreeningClient(cc)}, cc, nil
}

// Screen calls AmlKytScreening.Screen.
func (c *KytClient) Screen(ctx context.Context, req partner.KytRequest) (partner.KytResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, defaultCallTimeout)
	defer cancel()
	resp, err := c.cc.Screen(ctx, &pbkyt.KytRequest{
		TxId: req.TxID, UserId: req.UserID, DestAddress: req.DestAddress,
		Amount: req.Amount, Asset: req.Asset,
	})
	if err != nil {
		return partner.KytResponse{}, mapErr(err)
	}
	return partner.KytResponse{Decision: partner.KytDecision(resp.Decision), Reason: resp.Reason}, nil
}

// --- mpc ---------------------------------------------------------------------

// MpcClient wraps the generated gRPC client and implements partner.Mpc.
type MpcClient struct{ cc pbmpc.MpcSigningServiceClient }

// NewMpc dials target and returns an MpcClient.
func NewMpc(ctx context.Context, target string) (*MpcClient, *grpc.ClientConn, error) {
	creds, err := transportCredentials()
	if err != nil {
		return nil, nil, fmt.Errorf("mpc tls: %w", err)
	}
	cc, err := grpc.NewClient(target, grpc.WithTransportCredentials(creds))
	if err != nil {
		return nil, nil, fmt.Errorf("mpc dial %s: %w", target, err)
	}
	return &MpcClient{cc: pbmpc.NewMpcSigningServiceClient(cc)}, cc, nil
}

// Sign calls MpcSigningService.Sign.
func (c *MpcClient) Sign(ctx context.Context, req partner.MpcSignRequest) (partner.MpcSignResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, defaultCallTimeout)
	defer cancel()
	resp, err := c.cc.Sign(ctx, &pbmpc.MpcSignRequest{TxId: req.TxID, UnsignedTxHex: req.UnsignedTxHex})
	if err != nil {
		return partner.MpcSignResponse{}, mapErr(err)
	}
	return partner.MpcSignResponse{SignedTxHex: resp.SignedTxHex}, nil
}

// --- blockchain --------------------------------------------------------------

// BlockchainClient wraps the generated gRPC client and implements
// partner.Blockchain.
type BlockchainClient struct {
	cc pbblockchain.BlockchainGatewayClient
}

// NewBlockchain dials target and returns a BlockchainClient.
func NewBlockchain(ctx context.Context, target string) (*BlockchainClient, *grpc.ClientConn, error) {
	creds, err := transportCredentials()
	if err != nil {
		return nil, nil, fmt.Errorf("blockchain tls: %w", err)
	}
	cc, err := grpc.NewClient(target, grpc.WithTransportCredentials(creds))
	if err != nil {
		return nil, nil, fmt.Errorf("blockchain dial %s: %w", target, err)
	}
	return &BlockchainClient{cc: pbblockchain.NewBlockchainGatewayClient(cc)}, cc, nil
}

// Broadcast calls BlockchainGateway.Broadcast.
func (c *BlockchainClient) Broadcast(ctx context.Context, req partner.BroadcastRequest) (partner.BroadcastResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, defaultCallTimeout)
	defer cancel()
	resp, err := c.cc.Broadcast(ctx, &pbblockchain.BroadcastRequest{TxId: req.TxID, SignedTxHex: req.SignedTxHex})
	if err != nil {
		return partner.BroadcastResponse{}, mapErr(err)
	}
	return partner.BroadcastResponse{TxHash: resp.TxHash, InMempool: resp.InMempool, Confirmed: resp.Confirmed}, nil
}

// Status calls BlockchainGateway.Status.
func (c *BlockchainClient) Status(ctx context.Context, txHash string) (partner.BroadcastResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, defaultCallTimeout)
	defer cancel()
	resp, err := c.cc.Status(ctx, &pbblockchain.StatusRequest{TxHash: txHash})
	if err != nil {
		return partner.BroadcastResponse{}, mapErr(err)
	}
	return partner.BroadcastResponse{TxHash: resp.TxHash, InMempool: resp.InMempool, Confirmed: resp.Confirmed}, nil
}

// --- ledger ------------------------------------------------------------------

// LedgerClient wraps the generated gRPC client and implements partner.Ledger.
type LedgerClient struct {
	cc pbledger.LedgerAccountingClient
}

// NewLedger dials target and returns a LedgerClient.
func NewLedger(ctx context.Context, target string) (*LedgerClient, *grpc.ClientConn, error) {
	creds, err := transportCredentials()
	if err != nil {
		return nil, nil, fmt.Errorf("ledger tls: %w", err)
	}
	cc, err := grpc.NewClient(target, grpc.WithTransportCredentials(creds))
	if err != nil {
		return nil, nil, fmt.Errorf("ledger dial %s: %w", target, err)
	}
	return &LedgerClient{cc: pbledger.NewLedgerAccountingClient(cc)}, cc, nil
}

// PostDoubleEntry calls LedgerAccounting.PostDoubleEntry.
func (c *LedgerClient) PostDoubleEntry(ctx context.Context, req partner.LedgerPostRequest) (partner.LedgerPostResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, defaultCallTimeout)
	defer cancel()
	resp, err := c.cc.PostDoubleEntry(ctx, &pbledger.LedgerPostRequest{
		TxId: req.TxID, UserId: req.UserID, Amount: req.Amount, Asset: req.Asset, Rail: req.Rail,
	})
	if err != nil {
		return partner.LedgerPostResponse{}, mapErr(err)
	}
	return partner.LedgerPostResponse{JournalID: resp.JournalId}, nil
}
