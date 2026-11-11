// Package mysqldb opens the MySQL connection pool used by the writer and reader
package mysqldb

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"time"

	"github.com/go-sql-driver/mysql"
)

// Config is what open needs to reach MySQL and size the pool.
type Config struct {
	Host         string
	Port         string
	Database     string
	User         string
	Password     string
	MaxOpenConns int
}

// Open builds the pool and pings MySQL once, so a bad config or a down
// database fails at startup instead of on the first request.
func Open(ctx context.Context, cfg Config) (*sql.DB, error) {
	dc := mysql.NewConfig()
	dc.Net = "tcp"
	dc.Addr = net.JoinHostPort(cfg.Host, cfg.Port)
	dc.DBName = cfg.Database
	dc.User = cfg.User
	dc.Passwd = cfg.Password
	dc.ParseTime = true // TIMESTAMP columns scan into time.Time, not []byte
	dc.Loc = time.UTC
	dc.Params = map[string]string{"time_zone": "'+00:00'"} // session in UTC, whatever the server default

	connector, err := mysql.NewConnector(dc)
	if err != nil {
		return nil, err
	}
	db := sql.OpenDB(connector) // no connection is made

	db.SetMaxOpenConns(cfg.MaxOpenConns)
	db.SetMaxIdleConns(cfg.MaxOpenConns)
	db.SetConnMaxLifetime(3 * time.Minute) // stay under server and proxy timeouts

	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := db.PingContext(pingCtx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("mysql ping %s: %w", dc.Addr, err)
	}
	return db, nil
}
