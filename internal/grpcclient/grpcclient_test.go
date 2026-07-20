package grpcclient

import (
	"context"
	"errors"
	"net"
	"testing"

	"github.com/ai-crypto-onramp/transaction-orchestrator/internal/partner"
	pbblockchain "github.com/ai-crypto-onramp/transaction-orchestrator/internal/pb/blockchain"
	pbkyt "github.com/ai-crypto-onramp/transaction-orchestrator/internal/pb/kyt"
	pbledger "github.com/ai-crypto-onramp/transaction-orchestrator/internal/pb/ledger"
	pbmpc "github.com/ai-crypto-onramp/transaction-orchestrator/internal/pb/mpc"
	pbpayment "github.com/ai-crypto-onramp/transaction-orchestrator/internal/pb/payment"
	pbpolicy "github.com/ai-crypto-onramp/transaction-orchestrator/internal/pb/policy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

const bufSize = 1024 * 1024

// --- test servers ------------------------------------------------------------

type policyServer struct {
	pbpolicy.UnimplementedPolicyRiskEngineServer
	decision string
	reason   string
	err      error
	last     *pbpolicy.PolicyRequest
}

func (s *policyServer) Evaluate(ctx context.Context, req *pbpolicy.PolicyRequest) (*pbpolicy.PolicyResponse, error) {
	s.last = req
	if s.err != nil {
		return nil, s.err
	}
	return &pbpolicy.PolicyResponse{Decision: s.decision, Reason: s.reason}, nil
}

type paymentServer struct {
	pbpayment.UnimplementedPaymentOrchestrationServer
	authID    string
	captureID string
	err       error
	lastAuth  *pbpayment.PaymentAuthorizeRequest
	lastCap   *pbpayment.PaymentCaptureRequest
	lastVoid  *pbpayment.PaymentVoidRequest
	lastRef   *pbpayment.PaymentRefundRequest
	voidN     int
	refN      int
}

func (s *paymentServer) Authorize(ctx context.Context, req *pbpayment.PaymentAuthorizeRequest) (*pbpayment.PaymentAuthorizeResponse, error) {
	s.lastAuth = req
	if s.err != nil {
		return nil, s.err
	}
	return &pbpayment.PaymentAuthorizeResponse{AuthId: s.authID}, nil
}

func (s *paymentServer) Capture(ctx context.Context, req *pbpayment.PaymentCaptureRequest) (*pbpayment.PaymentCaptureResponse, error) {
	s.lastCap = req
	if s.err != nil {
		return nil, s.err
	}
	return &pbpayment.PaymentCaptureResponse{CaptureId: s.captureID}, nil
}

func (s *paymentServer) VoidAuthorization(ctx context.Context, req *pbpayment.PaymentVoidRequest) (*pbpayment.PaymentVoidResponse, error) {
	s.lastVoid = req
	s.voidN++
	if s.err != nil {
		return nil, s.err
	}
	return &pbpayment.PaymentVoidResponse{}, nil
}

func (s *paymentServer) Refund(ctx context.Context, req *pbpayment.PaymentRefundRequest) (*pbpayment.PaymentRefundResponse, error) {
	s.lastRef = req
	s.refN++
	if s.err != nil {
		return nil, s.err
	}
	return &pbpayment.PaymentRefundResponse{}, nil
}

type kytServer struct {
	pbkyt.UnimplementedAmlKytScreeningServer
	decision string
	reason   string
	err      error
	last     *pbkyt.KytRequest
}

func (s *kytServer) Screen(ctx context.Context, req *pbkyt.KytRequest) (*pbkyt.KytResponse, error) {
	s.last = req
	if s.err != nil {
		return nil, s.err
	}
	return &pbkyt.KytResponse{Decision: s.decision, Reason: s.reason}, nil
}

type mpcServer struct {
	pbmpc.UnimplementedMpcSigningServiceServer
	signed string
	err    error
	last   *pbmpc.MpcSignRequest
}

func (s *mpcServer) Sign(ctx context.Context, req *pbmpc.MpcSignRequest) (*pbmpc.MpcSignResponse, error) {
	s.last = req
	if s.err != nil {
		return nil, s.err
	}
	return &pbmpc.MpcSignResponse{SignedTxHex: s.signed}, nil
}

type blockchainServer struct {
	pbblockchain.UnimplementedBlockchainGatewayServer
	txHash    string
	inMempool bool
	confirmed bool
	err       error
	lastB     *pbblockchain.BroadcastRequest
	lastS     *pbblockchain.StatusRequest
}

func (s *blockchainServer) Broadcast(ctx context.Context, req *pbblockchain.BroadcastRequest) (*pbblockchain.BroadcastResponse, error) {
	s.lastB = req
	if s.err != nil {
		return nil, s.err
	}
	return &pbblockchain.BroadcastResponse{TxHash: s.txHash, InMempool: s.inMempool, Confirmed: s.confirmed}, nil
}

func (s *blockchainServer) Status(ctx context.Context, req *pbblockchain.StatusRequest) (*pbblockchain.BroadcastResponse, error) {
	s.lastS = req
	if s.err != nil {
		return nil, s.err
	}
	return &pbblockchain.BroadcastResponse{TxHash: s.txHash, InMempool: s.inMempool, Confirmed: s.confirmed}, nil
}

type ledgerServer struct {
	pbledger.UnimplementedLedgerAccountingServer
	journalID string
	err       error
	last      *pbledger.LedgerPostRequest
}

func (s *ledgerServer) PostDoubleEntry(ctx context.Context, req *pbledger.LedgerPostRequest) (*pbledger.LedgerPostResponse, error) {
	s.last = req
	if s.err != nil {
		return nil, s.err
	}
	return &pbledger.LedgerPostResponse{JournalId: s.journalID}, nil
}

// --- helpers -----------------------------------------------------------------

// newClientConn creates a bufconn-backed ClientConn that can be passed to the
// generated pb.NewXClient constructors, which is how the wrapper clients are
// built in the package's New* functions. The returned conn must be closed.
func newClientConn(t *testing.T, register func(s *grpc.Server)) *grpc.ClientConn {
	t.Helper()
	lis := bufconn.Listen(bufSize)
	srv := grpc.NewServer()
	register(srv)
	go func() { _ = srv.Serve(lis) }()
	cc, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		srv.Stop()
		t.Fatalf("grpc.NewClient: %v", err)
	}
	t.Cleanup(func() {
		_ = cc.Close()
		srv.Stop()
	})
	return cc
}

// --- mapErr ------------------------------------------------------------------

func TestMapErr(t *testing.T) {
	cases := []struct {
		name string
		in   error
		want error
	}{
		{"nil", nil, nil},
		{"non-grpc", errors.New("boom"), errors.New("boom")},
		{"denied", status.Error(codes.PermissionDenied, "nope"), partner.ErrDenied},
		{"failed-precondition", status.Error(codes.FailedPrecondition, "x"), partner.ErrDenied},
		{"unavailable", status.Error(codes.Unavailable, "down"), partner.ErrTransient},
		{"deadline-exceeded", status.Error(codes.DeadlineExceeded, "slow"), partner.ErrTransient},
		{"other-code", status.Error(codes.NotFound, "meh"), status.Error(codes.NotFound, "meh")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := mapErr(tc.in)
			switch {
			case tc.in == nil:
				if got != nil {
					t.Fatalf("expected nil, got %v", got)
				}
			case errors.Is(tc.want, partner.ErrDenied):
				if !errors.Is(got, partner.ErrDenied) {
					t.Fatalf("expected ErrDenied, got %v", got)
				}
			case errors.Is(tc.want, partner.ErrTransient):
				if !errors.Is(got, partner.ErrTransient) {
					t.Fatalf("expected ErrTransient, got %v", got)
				}
			default:
				// For non-grpc / other codes we expect the original error
				// to be returned unchanged (by identity for non-grpc, by
				// status for other codes).
				if got == nil {
					t.Fatalf("expected non-nil error")
				}
			}
		})
	}
}

// --- PolicyClient ------------------------------------------------------------

func TestPolicyClientEvaluateSuccessAndMapping(t *testing.T) {
	srv := &policyServer{decision: "allow", reason: "ok"}
	cc := newClientConn(t, func(s *grpc.Server) {
		pbpolicy.RegisterPolicyRiskEngineServer(s, srv)
	})
	c := &PolicyClient{cc: pbpolicy.NewPolicyRiskEngineClient(cc)}

	resp, err := c.Evaluate(context.Background(), partner.PolicyRequest{
		TxID: "tx1", UserID: "u1", QuoteID: "q1", Amount: "100", Asset: "BTC", Rail: "CARD", DestAddress: "0xabc",
	})
	if err != nil {
		t.Fatalf("Evaluate: %v", err)
	}
	if resp.Decision != partner.PolicyAllow || resp.Reason != "ok" {
		t.Fatalf("unexpected response: %+v", resp)
	}
	if srv.last == nil || srv.last.TxId != "tx1" || srv.last.UserId != "u1" || srv.last.DestAddress != "0xabc" {
		t.Fatalf("server received wrong request: %+v", srv.last)
	}

	// Denied mapping.
	srv2 := &policyServer{err: status.Error(codes.PermissionDenied, "no")}
	cc2 := newClientConn(t, func(s *grpc.Server) {
		pbpolicy.RegisterPolicyRiskEngineServer(s, srv2)
	})
	c2 := &PolicyClient{cc: pbpolicy.NewPolicyRiskEngineClient(cc2)}
	if _, err := c2.Evaluate(context.Background(), partner.PolicyRequest{TxID: "t"}); !errors.Is(err, partner.ErrDenied) {
		t.Fatalf("expected ErrDenied, got %v", err)
	}
}

// --- PaymentClient -----------------------------------------------------------

func TestPaymentClientHappyAndErrors(t *testing.T) {
	srv := &paymentServer{authID: "auth-1", captureID: "cap-1"}
	cc := newClientConn(t, func(s *grpc.Server) {
		pbpayment.RegisterPaymentOrchestrationServer(s, srv)
	})
	c := &PaymentClient{cc: pbpayment.NewPaymentOrchestrationClient(cc)}

	auth, err := c.Authorize(context.Background(), partner.PaymentAuthorizeRequest{
		TxID: "tx", UserID: "u", QuoteID: "q", Amount: "1", Asset: "BTC", Rail: "CARD",
	})
	if err != nil || auth.AuthID != "auth-1" {
		t.Fatalf("Authorize: %+v %v", auth, err)
	}
	if srv.lastAuth.TxId != "tx" || srv.lastAuth.Amount != "1" {
		t.Fatalf("server got %+v", srv.lastAuth)
	}

	cap, err := c.Capture(context.Background(), partner.PaymentCaptureRequest{TxID: "tx", AuthID: "a", Amount: "1", Asset: "BTC"})
	if err != nil || cap.CaptureID != "cap-1" {
		t.Fatalf("Capture: %+v %v", cap, err)
	}
	if srv.lastCap.AuthId != "a" {
		t.Fatalf("server got %+v", srv.lastCap)
	}

	if err := c.VoidAuthorization(context.Background(), partner.PaymentVoidRequest{TxID: "tx", AuthID: "a"}); err != nil {
		t.Fatalf("Void: %v", err)
	}
	if srv.lastVoid.AuthId != "a" || srv.voidN != 1 {
		t.Fatalf("void: %+v n=%d", srv.lastVoid, srv.voidN)
	}

	if err := c.Refund(context.Background(), partner.PaymentRefundRequest{TxID: "tx", CaptureID: "c", Amount: "1", Asset: "BTC"}); err != nil {
		t.Fatalf("Refund: %v", err)
	}
	if srv.lastRef.CaptureId != "c" || srv.refN != 1 {
		t.Fatalf("refund: %+v n=%d", srv.lastRef, srv.refN)
	}

	// Transient error mapping on Capture.
	srv2 := &paymentServer{err: status.Error(codes.Unavailable, "down")}
	cc2 := newClientConn(t, func(s *grpc.Server) {
		pbpayment.RegisterPaymentOrchestrationServer(s, srv2)
	})
	c2 := &PaymentClient{cc: pbpayment.NewPaymentOrchestrationClient(cc2)}
	if _, err := c2.Capture(context.Background(), partner.PaymentCaptureRequest{TxID: "t"}); !errors.Is(err, partner.ErrTransient) {
		t.Fatalf("expected ErrTransient, got %v", err)
	}
	if err := c2.VoidAuthorization(context.Background(), partner.PaymentVoidRequest{TxID: "t"}); !errors.Is(err, partner.ErrTransient) {
		t.Fatalf("expected ErrTransient on void, got %v", err)
	}
	if err := c2.Refund(context.Background(), partner.PaymentRefundRequest{TxID: "t"}); !errors.Is(err, partner.ErrTransient) {
		t.Fatalf("expected ErrTransient on refund, got %v", err)
	}
	if _, err := c2.Authorize(context.Background(), partner.PaymentAuthorizeRequest{TxID: "t"}); !errors.Is(err, partner.ErrTransient) {
		t.Fatalf("expected ErrTransient on auth, got %v", err)
	}
}

// --- KytClient ---------------------------------------------------------------

func TestKytClientScreen(t *testing.T) {
	srv := &kytServer{decision: "clear", reason: "ok"}
	cc := newClientConn(t, func(s *grpc.Server) { pbkyt.RegisterAmlKytScreeningServer(s, srv) })
	c := &KytClient{cc: pbkyt.NewAmlKytScreeningClient(cc)}

	resp, err := c.Screen(context.Background(), partner.KytRequest{TxID: "tx", UserID: "u", DestAddress: "0x", Amount: "1", Asset: "BTC"})
	if err != nil || resp.Decision != partner.KytClear || resp.Reason != "ok" {
		t.Fatalf("Screen: %+v %v", resp, err)
	}
	if srv.last.TxId != "tx" || srv.last.DestAddress != "0x" {
		t.Fatalf("server got %+v", srv.last)
	}

	// Denied mapping via FailedPrecondition.
	srv2 := &kytServer{err: status.Error(codes.FailedPrecondition, "x")}
	cc2 := newClientConn(t, func(s *grpc.Server) { pbkyt.RegisterAmlKytScreeningServer(s, srv2) })
	c2 := &KytClient{cc: pbkyt.NewAmlKytScreeningClient(cc2)}
	if _, err := c2.Screen(context.Background(), partner.KytRequest{TxID: "t"}); !errors.Is(err, partner.ErrDenied) {
		t.Fatalf("expected ErrDenied, got %v", err)
	}
}

// --- MpcClient ---------------------------------------------------------------

func TestMpcClientSign(t *testing.T) {
	srv := &mpcServer{signed: "0xsigned"}
	cc := newClientConn(t, func(s *grpc.Server) { pbmpc.RegisterMpcSigningServiceServer(s, srv) })
	c := &MpcClient{cc: pbmpc.NewMpcSigningServiceClient(cc)}

	resp, err := c.Sign(context.Background(), partner.MpcSignRequest{TxID: "tx", UnsignedTxHex: "0xu"})
	if err != nil || resp.SignedTxHex != "0xsigned" {
		t.Fatalf("Sign: %+v %v", resp, err)
	}
	if srv.last.TxId != "tx" || srv.last.UnsignedTxHex != "0xu" {
		t.Fatalf("server got %+v", srv.last)
	}

	srv2 := &mpcServer{err: status.Error(codes.Unavailable, "x")}
	cc2 := newClientConn(t, func(s *grpc.Server) { pbmpc.RegisterMpcSigningServiceServer(s, srv2) })
	c2 := &MpcClient{cc: pbmpc.NewMpcSigningServiceClient(cc2)}
	if _, err := c2.Sign(context.Background(), partner.MpcSignRequest{TxID: "t"}); !errors.Is(err, partner.ErrTransient) {
		t.Fatalf("expected ErrTransient, got %v", err)
	}
}

// --- BlockchainClient --------------------------------------------------------

func TestBlockchainClientBroadcastAndStatus(t *testing.T) {
	srv := &blockchainServer{txHash: "0xhash", inMempool: true, confirmed: false}
	cc := newClientConn(t, func(s *grpc.Server) { pbblockchain.RegisterBlockchainGatewayServer(s, srv) })
	c := &BlockchainClient{cc: pbblockchain.NewBlockchainGatewayClient(cc)}

	resp, err := c.Broadcast(context.Background(), partner.BroadcastRequest{TxID: "tx", SignedTxHex: "0xs"})
	if err != nil || resp.TxHash != "0xhash" || !resp.InMempool || resp.Confirmed {
		t.Fatalf("Broadcast: %+v %v", resp, err)
	}
	if srv.lastB.TxId != "tx" || srv.lastB.SignedTxHex != "0xs" {
		t.Fatalf("server got %+v", srv.lastB)
	}

	resp, err = c.Status(context.Background(), "0xhash")
	if err != nil || resp.TxHash != "0xhash" {
		t.Fatalf("Status: %+v %v", resp, err)
	}
	if srv.lastS == nil || srv.lastS.TxHash != "0xhash" {
		t.Fatalf("server got %+v", srv.lastS)
	}

	// Denied via FailedPrecondition.
	srv2 := &blockchainServer{err: status.Error(codes.FailedPrecondition, "x")}
	cc2 := newClientConn(t, func(s *grpc.Server) { pbblockchain.RegisterBlockchainGatewayServer(s, srv2) })
	c2 := &BlockchainClient{cc: pbblockchain.NewBlockchainGatewayClient(cc2)}
	if _, err := c2.Broadcast(context.Background(), partner.BroadcastRequest{TxID: "t"}); !errors.Is(err, partner.ErrDenied) {
		t.Fatalf("expected ErrDenied, got %v", err)
	}
	if _, err := c2.Status(context.Background(), "h"); !errors.Is(err, partner.ErrDenied) {
		t.Fatalf("expected ErrDenied on status, got %v", err)
	}
}

// --- LedgerClient ------------------------------------------------------------

func TestLedgerClientPostDoubleEntry(t *testing.T) {
	srv := &ledgerServer{journalID: "j-1"}
	cc := newClientConn(t, func(s *grpc.Server) { pbledger.RegisterLedgerAccountingServer(s, srv) })
	c := &LedgerClient{cc: pbledger.NewLedgerAccountingClient(cc)}

	resp, err := c.PostDoubleEntry(context.Background(), partner.LedgerPostRequest{TxID: "tx", UserID: "u", Amount: "1", Asset: "BTC", Rail: "CARD"})
	if err != nil || resp.JournalID != "j-1" {
		t.Fatalf("PostDoubleEntry: %+v %v", resp, err)
	}
	if srv.last.TxId != "tx" || srv.last.UserId != "u" || srv.last.Rail != "CARD" {
		t.Fatalf("server got %+v", srv.last)
	}

	srv2 := &ledgerServer{err: status.Error(codes.Unavailable, "x")}
	cc2 := newClientConn(t, func(s *grpc.Server) { pbledger.RegisterLedgerAccountingServer(s, srv2) })
	c2 := &LedgerClient{cc: pbledger.NewLedgerAccountingClient(cc2)}
	if _, err := c2.PostDoubleEntry(context.Background(), partner.LedgerPostRequest{TxID: "t"}); !errors.Is(err, partner.ErrTransient) {
		t.Fatalf("expected ErrTransient, got %v", err)
	}
}

// --- New* constructors dial a target -----------------------------------------
// The New* functions dial a target string with grpc.NewClient. We exercise
// the success path (no error returned for a syntactically-valid target) and
// verify the returned conn is non-nil. We do not actually serve traffic; the
// dial is lazy (grpc.NewClient does not block on a connection).

func TestNewConstructorsReturnClientAndConn(t *testing.T) {
	ctx := context.Background()
	target := "passthrough://localhost:1234"
	ctors := []struct {
		name string
		fn   func(context.Context, string) (any, *grpc.ClientConn, error)
	}{
		{"policy", func(c context.Context, s string) (any, *grpc.ClientConn, error) {
			return NewPolicy(c, s)
		}},
		{"payment", func(c context.Context, s string) (any, *grpc.ClientConn, error) {
			return NewPayment(c, s)
		}},
		{"kyt", func(c context.Context, s string) (any, *grpc.ClientConn, error) {
			return NewKyt(c, s)
		}},
		{"mpc", func(c context.Context, s string) (any, *grpc.ClientConn, error) {
			return NewMpc(c, s)
		}},
		{"blockchain", func(c context.Context, s string) (any, *grpc.ClientConn, error) {
			return NewBlockchain(c, s)
		}},
		{"ledger", func(c context.Context, s string) (any, *grpc.ClientConn, error) {
			return NewLedger(c, s)
		}},
	}
	for _, ct := range ctors {
		t.Run(ct.name, func(t *testing.T) {
			cli, conn, err := ct.fn(ctx, target)
			if err != nil {
				t.Fatalf("%s: %v", ct.name, err)
			}
			if cli == nil || conn == nil {
				t.Fatalf("%s: nil client/conn", ct.name)
			}
			_ = conn.Close()
		})
	}
}