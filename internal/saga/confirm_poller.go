package saga

import (
	"context"
	"errors"
	"time"

	"github.com/ai-crypto-onramp/transaction-orchestrator/internal/logging"
	"github.com/ai-crypto-onramp/transaction-orchestrator/internal/statemachine"
	"github.com/ai-crypto-onramp/transaction-orchestrator/internal/store"
)

// ConfirmPoller advances a saga from StateBroadcasted to StateConfirmed by
// polling the blockchain-gateway.  It is optional: the BroadcastStep may
// already report Confirmed (in which case the poller is a no-op).
type ConfirmPoller struct {
	Store    store.Store
	Client   partnerBlockchain
	Interval time.Duration
	MaxWait  time.Duration
}

// partnerBlockchain is the subset of partner.Blockchain used here.
type partnerBlockchain interface {
	Status(ctx context.Context, txHash string) (broadcastStatus, error)
}

// broadcastStatus is the subset of partner.BroadcastResponse used here.
type broadcastStatus struct {
	Confirmed bool
	TxHash    string
}

// Run polls until the tx is confirmed or ctx is cancelled.
func (p *ConfirmPoller) Run(ctx context.Context, txID string) error {
	log := logging.From(ctx)
	deadline := time.Now().Add(p.MaxWait)
	tick := time.NewTicker(p.Interval)
	defer tick.Stop()
	for {
		sg, err := p.Store.LoadSagaState(ctx, txID)
		if err != nil {
			return err
		}
		if sg.State == statemachine.StateConfirmed || sg.State == statemachine.StateLedgered || sg.State == statemachine.StateCompleted {
			return nil
		}
		if sg.State != statemachine.StateBroadcasted {
			return errors.New("confirm: saga not in broadcasted state")
		}
		txHash, _ := sg.Payload["tx_hash"].(string)
		if txHash == "" {
			return errors.New("confirm: missing tx_hash in saga payload")
		}
		st, err := p.Client.Status(ctx, txHash)
		if err == nil && st.Confirmed {
			return p.persistConfirmed(ctx, txID, sg)
		}
		if time.Now().After(deadline) {
			log.Warn("confirm poller deadline exceeded", "tx_id", txID)
			return errors.New("confirm: deadline exceeded")
		}
		select {
		case <-tick.C:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (p *ConfirmPoller) persistConfirmed(ctx context.Context, txID string, sg store.SagaState) error {
	tx, err := p.Store.LoadTx(ctx, txID)
	if err != nil {
		return err
	}
	return p.Store.RunInTx(ctx, func(ts store.TxStore) error {
		newSaga := sg
		newSaga.State = statemachine.StateConfirmed
		newSaga.CurrentStep = statemachine.StepLedger
		newSaga.Version = sg.Version + 1
		if _, err := statemachine.Transition(sg.State, newSaga.State); err != nil {
			return err
		}
		if err := ts.SaveSagaState(ctx, newSaga); err != nil {
			return err
		}
		if err := ts.UpdateTransactionStatus(ctx, txID, newSaga.State, tx.Version); err != nil {
			return err
		}
		evtType := "transaction.confirmed"
		_ = ts.AppendOutbox(ctx, []store.OutboxEvent{{
			EventID: store.NewEventID(), TxID: txID, EventType: evtType,
			Status: store.OutboxPending, DedupKey: store.DedupKey(txID, evtType, "", 0),
			CreatedAt: time.Now().UTC(),
			Payload:   map[string]any{"tx_id": txID, "tx_hash": sg.Payload["tx_hash"]},
		}})
		return nil
	})
}