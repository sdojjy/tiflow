// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package middleware

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/api"
	"github.com/pingcap/tiflow/cdc/capture"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/upstream"
	"go.uber.org/zap"
)

// ClientVersionHeader is the header name of client version
const ClientVersionHeader = "X-client-version"

// LogMiddleware logs the api requests
func LogMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		path := c.Request.URL.Path
		query := c.Request.URL.RawQuery
		user, _, _ := c.Request.BasicAuth()
		c.Next()

		cost := time.Since(start)

		err := c.Errors.Last()
		var stdErr error
		if err != nil {
			stdErr = err.Err
		}
		version := c.Request.Header.Get(ClientVersionHeader)
		log.Info("cdc open api request",
			zap.Int("status", c.Writer.Status()),
			zap.String("method", c.Request.Method),
			zap.String("path", path),
			zap.String("query", query),
			zap.String("ip", c.ClientIP()),
			zap.String("user-agent", c.Request.UserAgent()), zap.String("client-version", version),
			zap.String("username", user),
			zap.Error(stdErr),
			zap.Duration("duration", cost),
		)
	}
}

// ErrorHandleMiddleware puts the error into response
func ErrorHandleMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Next()
		// because we will return immediately after an error occurs in http_handler
		// there wil be only one error in c.Errors
		lastError := c.Errors.Last()
		if lastError != nil {
			err := lastError.Err
			// put the error into response
			if api.IsHTTPBadRequestError(err) {
				c.IndentedJSON(http.StatusBadRequest, model.NewHTTPError(err))
			} else {
				c.IndentedJSON(http.StatusInternalServerError, model.NewHTTPError(err))
			}
			c.Abort()
			return
		}
	}
}

// ForwardToOwnerMiddleware forward a request to controller if current server
// is not controller, or handle it locally.
func ForwardToOwnerMiddleware(p capture.Capture) gin.HandlerFunc {
	return func(ctx *gin.Context) {
		if !p.IsOwner() {
			api.ForwardToOwner(ctx, p)

			// Without calling Abort(), Gin will continue to process the next handler,
			// execute code which should only be run by the owner, and cause a panic.
			// See https://github.com/pingcap/tiflow/issues/5888
			ctx.Abort()
			return
		}
		ctx.Next()
	}
}

// CheckServerReadyMiddleware checks if the server is ready
func CheckServerReadyMiddleware(capture capture.Capture) gin.HandlerFunc {
	return func(c *gin.Context) {
		if capture.IsReady() {
			c.Next()
		} else {
			c.IndentedJSON(http.StatusServiceUnavailable,
				model.NewHTTPError(errors.ErrServerIsNotReady))
			c.Abort()
			return
		}
	}
}

// AuthenticateMiddleware authenticates the request by query upstream TiDB.
func AuthenticateMiddleware(capture capture.Capture) gin.HandlerFunc {
	return func(ctx *gin.Context) {
		serverCfg := config.GetGlobalServerConfig()
		if serverCfg.Security.ClientUserRequired {
			up, err := getUpstream(capture)
			if err != nil {
				_ = ctx.Error(err)
				ctx.Abort()
				return
			}

			if err := verify(ctx, up); err != nil {
				ctx.IndentedJSON(http.StatusUnauthorized, model.NewHTTPError(err))
				ctx.Abort()
				return
			}
		}
		ctx.Next()
	}
}

func getUpstream(capture capture.Capture) (*upstream.Upstream, error) {
	m, err := capture.GetUpstreamManager()
	if err != nil {
		return nil, errors.Trace(err)
	}
	return m.GetDefaultUpstream()
}

func verify(ctx *gin.Context, up *upstream.Upstream) error {
	// get the username and password from the authorization header
	username, password, ok := ctx.Request.BasicAuth()
	if !ok {
		errMsg := "please specify the user and password via authorization header"
		return errors.ErrCredentialNotFound.GenWithStackByArgs(errMsg)
	}

	allowed := false
	serverCfg := config.GetGlobalServerConfig()
	for _, user := range serverCfg.Security.ClientAllowedUser {
		if user == username {
			allowed = true
			break
		}
	}
	if !allowed {
		errMsg := "The user is not allowed."
		if username == "" {
			errMsg = "Empty username is not allowed."
		}
		return errors.ErrUnauthorized.GenWithStackByArgs(username, errMsg)
	}
	if err := up.VerifyTiDBUser(ctx, username, password); err != nil {
		return errors.ErrUnauthorized.GenWithStackByArgs(username, err.Error())
	}
	return nil
}
