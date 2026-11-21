package migrate

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestApplyPostgresCredentials(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		uri      string
		username string
		password string
		wantUser *url.Userinfo
		wantErr  bool
	}{
		{
			name:     "empty_flags_do_not_inject_empty_userinfo",
			uri:      "postgres://localhost:5432/postgres",
			username: "",
			password: "",
			wantUser: nil,
		},
		{
			name:     "empty_flags_preserve_username_only_uri",
			uri:      "postgres://alice@localhost:5432/postgres",
			username: "",
			password: "",
			wantUser: url.User("alice"),
		},
		{
			name:     "empty_flags_preserve_uri_userinfo",
			uri:      "postgres://alice:secret@localhost:5432/postgres",
			username: "",
			password: "",
			wantUser: url.UserPassword("alice", "secret"),
		},
		{
			name:     "username_flag_overrides_uri_user",
			uri:      "postgres://alice:secret@localhost:5432/postgres",
			username: "bob",
			password: "",
			wantUser: url.UserPassword("bob", "secret"),
		},
		{
			name:     "password_flag_overrides_uri_password",
			uri:      "postgres://alice:secret@localhost:5432/postgres",
			username: "",
			password: "newpass",
			wantUser: url.UserPassword("alice", "newpass"),
		},
		{
			name:     "both_flags_override_uri_userinfo",
			uri:      "postgres://alice:secret@localhost:5432/postgres",
			username: "bob",
			password: "newpass",
			wantUser: url.UserPassword("bob", "newpass"),
		},
		{
			name:     "username_flag_on_userinfo_less_uri",
			uri:      "postgres://localhost:5432/postgres",
			username: "bob",
			password: "",
			wantUser: url.User("bob"),
		},
		{
			name:     "password_flag_on_userinfo_less_uri",
			uri:      "postgres://localhost:5432/postgres",
			username: "",
			password: "newpass",
			wantUser: url.UserPassword("", "newpass"),
		},
		{
			name:     "invalid_uri",
			uri:      "://",
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := applyPostgresCredentials(tt.uri, tt.username, tt.password)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)

			parsed, err := url.Parse(got)
			require.NoError(t, err)
			require.Equal(t, tt.wantUser, parsed.User)
		})
	}
}
