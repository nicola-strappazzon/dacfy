package pipelines_test

import (
	"testing"

	"github.com/nicola-strappazzon/dacfy/pipelines"

	"github.com/stretchr/testify/assert"
)

func TestUser_Create(t *testing.T) {
	t.Run("with name and password", func(t *testing.T) {
		u := pipelines.User{
			Name:     pipelines.Name("malware"),
			Password: "EWZJcEvRZg9zfsg1",
		}

		assert.Equal(t, "CREATE USER IF NOT EXISTS malware IDENTIFIED BY 'EWZJcEvRZg9zfsg1'", u.Create().SQL())
		assert.Empty(t, u.SQL())
	})

	t.Run("without password", func(t *testing.T) {
		u := pipelines.User{
			Name: pipelines.Name("malware"),
		}

		assert.Equal(t, "CREATE USER IF NOT EXISTS malware", u.Create().SQL())
	})

	t.Run("with cluster", func(t *testing.T) {
		u := pipelines.User{
			Cluster:  pipelines.Name("zynap_prd"),
			Name:     pipelines.Name("malware"),
			Password: "EWZJcEvRZg9zfsg1",
		}

		assert.Equal(t, "CREATE USER IF NOT EXISTS malware ON CLUSTER zynap_prd IDENTIFIED BY 'EWZJcEvRZg9zfsg1'", u.Create().SQL())
	})

	t.Run("empty name -> no statement", func(t *testing.T) {
		u := pipelines.User{}

		assert.Empty(t, u.Create().SQL())
	})
}

func TestUser_Grant(t *testing.T) {
	t.Run("simple grant", func(t *testing.T) {
		u := pipelines.User{
			Name: pipelines.Name("malware"),
		}

		g := pipelines.UserGrant{Privilege: "SELECT", On: "malware_search.*"}

		assert.Equal(t, "GRANT SELECT ON malware_search.* TO malware", u.Grant(g).SQL())
	})

	t.Run("grant on cluster", func(t *testing.T) {
		u := pipelines.User{
			Cluster: pipelines.Name("zynap_prd"),
			Name:    pipelines.Name("malware"),
		}

		g := pipelines.UserGrant{Privilege: "SHOW", On: "malware_search.*"}

		assert.Equal(t, "GRANT ON CLUSTER zynap_prd SHOW ON malware_search.* TO malware", u.Grant(g).SQL())
	})

	t.Run("empty name -> no statement", func(t *testing.T) {
		u := pipelines.User{}

		g := pipelines.UserGrant{Privilege: "SELECT", On: "malware_search.*"}

		assert.Empty(t, u.Grant(g).SQL())
	})
}

func TestUser_Drop(t *testing.T) {
	t.Run("with name", func(t *testing.T) {
		u := pipelines.User{
			Name: pipelines.Name("malware"),
		}

		assert.Equal(t, "DROP USER IF EXISTS malware", u.Drop().SQL())
	})

	t.Run("with cluster", func(t *testing.T) {
		u := pipelines.User{
			Cluster: pipelines.Name("zynap_prd"),
			Name:    pipelines.Name("malware"),
		}

		assert.Equal(t, "DROP USER IF EXISTS malware ON CLUSTER zynap_prd", u.Drop().SQL())
	})

	t.Run("empty name -> no statement", func(t *testing.T) {
		u := pipelines.User{}

		assert.Empty(t, u.Drop().SQL())
	})
}

func TestUser_Validate(t *testing.T) {
	t.Run("empty user -> ok (optional block)", func(t *testing.T) {
		u := pipelines.User{}
		assert.NoError(t, u.Validate())
	})

	t.Run("valid user with grants", func(t *testing.T) {
		u := pipelines.User{
			Cluster:  pipelines.Name("zynap_prd"),
			Name:     pipelines.Name("malware"),
			Password: "EWZJcEvRZg9zfsg1",
			Grants: []pipelines.UserGrant{
				{Privilege: "SHOW", On: "malware_search.*"},
				{Privilege: "SELECT", On: "malware_search.*"},
			},
		}
		assert.NoError(t, u.Validate())
	})

	t.Run("invalid name", func(t *testing.T) {
		u := pipelines.User{
			Name: pipelines.Name("mal-ware"),
		}
		e := u.Validate()
		assert.Error(t, e)
		assert.Equal(t, `user.name "mal-ware" is invalid; must start with a letter and contain only letters, digits or underscores (max 255 characters)`, e.Error())
	})

	t.Run("invalid cluster", func(t *testing.T) {
		u := pipelines.User{
			Cluster: pipelines.Name("zynap-prd"),
			Name:    pipelines.Name("malware"),
		}
		e := u.Validate()
		assert.Error(t, e)
		assert.Equal(t, `user.cluster "zynap-prd" is invalid; must start with a letter and contain only letters, digits or underscores (max 255 characters)`, e.Error())
	})

	t.Run("grant missing privilege", func(t *testing.T) {
		u := pipelines.User{
			Name:   pipelines.Name("malware"),
			Grants: []pipelines.UserGrant{{On: "malware_search.*"}},
		}
		e := u.Validate()
		assert.Error(t, e)
		assert.Equal(t, "user.grants.privilege is required", e.Error())
	})

	t.Run("grant missing on", func(t *testing.T) {
		u := pipelines.User{
			Name:   pipelines.Name("malware"),
			Grants: []pipelines.UserGrant{{Privilege: "SELECT"}},
		}
		e := u.Validate()
		assert.Error(t, e)
		assert.Equal(t, "user.grants.on is required", e.Error())
	})
}
