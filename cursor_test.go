package dsent

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEncryptDecrypt(t *testing.T) {
	keys := []string{
		"",
		"test",
		"testtesttesttesttesttesttesttest",
	}

	msgs := []string{
		"",
		"test",
		"testtesttesttesttesttesttesttest",
	}

	for _, key := range keys {
		for _, msg := range msgs {
			enc, err := EncryptMessage(key, msg)
			require.NoError(t, err)

			dec, err := DecryptMessage(key, enc)
			require.NoError(t, err)

			require.Equal(t, msg, dec)
		}
	}
}
