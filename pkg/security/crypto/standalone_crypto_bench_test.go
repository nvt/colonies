package crypto

import "testing"

// BenchmarkRecoverID measures the per-message cost the server pays to
// authenticate every RPC: a secp256k1 ECDSA public-key recovery. Every log line
// is an individually signed RPC, so this is paid once per line today. Batching N
// lines into one signed request amortizes it to once per batch.
//
// Run: go test -bench=BenchmarkRecoverID -benchmem ./pkg/security/crypto/
func BenchmarkRecoverID(b *testing.B) {
	crypto := CreateCrypto()
	prvKey, err := crypto.GeneratePrivateKey()
	if err != nil {
		b.Fatal(err)
	}

	// A payload roughly the size of an add_log RPC body.
	msg := `{"processid":"d8f3...e21a","message":"2026-06-20T10:00:00Z INFO processing chunk 4213 of 9000, throughput=123MB/s","msgtype":"addlogmsg"}`
	sig, err := crypto.GenerateSignature(msg, prvKey)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := crypto.RecoverID(msg, sig); err != nil {
			b.Fatal(err)
		}
	}
}
