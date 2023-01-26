package orm

import (
	"database/sql"
	"fmt"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

type Config struct {
	Host            string
	Port            int
	Username        string
	Password        string
	DBName          string
	DBDriver        string
	MaxOpenConn     int
	MaxIdleConn     int
	ConnMaxLifeTime int
	ConnMaxIdleTime int
	DSNParams       string
}

type Client struct {
	cfg *Config
}

func NewClient(cfg *Config) *Client {
	c := new(Client)
	c.cfg = cfg
	return c
}

func (c *Client) Connect() (*DB, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", c.cfg.Username, c.cfg.Password, c.cfg.Host, c.cfg.Port, c.cfg.DBName)
	if c.cfg.DSNParams != "" {
		dsn += "?" + c.cfg.DSNParams
	}
	db, err := sql.Open(c.cfg.DBDriver, dsn)
	if err != nil {
		return nil, err
	}

	db.SetConnMaxLifetime(time.Duration(c.cfg.ConnMaxLifeTime) * time.Millisecond)
	db.SetConnMaxIdleTime(time.Duration(c.cfg.ConnMaxIdleTime) * time.Millisecond)
	db.SetMaxOpenConns(c.cfg.MaxOpenConn)
	db.SetMaxIdleConns(c.cfg.MaxIdleConn)
	return db, err
}
