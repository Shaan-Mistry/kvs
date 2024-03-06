package main

import (
	"encoding/json"
	"io"
	"net/http"

	"github.com/labstack/echo/v4"
)

// Define Valid JSON format for View Requests
type View_Request struct {
	SocketAdress string `json:"socket-address"`
	FromRepilca  string `json:"from-replica,omitempty"`
}

// PUT /view
func putReplicaView(c echo.Context) error {
	body, err := io.ReadAll(c.Request().Body)
	if err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "Failed to read request body"})
	}
	var viewRequest View_Request
	if err := json.Unmarshal(body, &viewRequest); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "Invalid JSON format"})
	}
	for _, addr := range CURRENT_VIEW {
		if addr == viewRequest.SocketAdress {
			return c.JSON(http.StatusOK, map[string]string{"result": "already present"})
		}
	}
	CURRENT_VIEW = append(CURRENT_VIEW, viewRequest.SocketAdress)
	MY_VECTOR_CLOCK.Set(viewRequest.SocketAdress, 0)
	return c.JSON(http.StatusCreated, map[string]string{"result": "added"})
}

// GET /view
func getView(c echo.Context) error {
	return c.JSON(http.StatusOK, map[string][]string{"view": CURRENT_VIEW})
}

// DELETE /view
func deleteReplicaView(c echo.Context) error {
	body, err := io.ReadAll(c.Request().Body)
	if err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "Failed to read request body"})
	}
	var viewRequest View_Request
	if err := json.Unmarshal(body, &viewRequest); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "Invalid JSON format"})
	}
	for i, addr := range CURRENT_VIEW {
		if addr == viewRequest.SocketAdress {
			// Remove the address from the view
			CURRENT_VIEW = append(CURRENT_VIEW[:i], CURRENT_VIEW[i+1:]...)
			return c.JSON(http.StatusOK, map[string]string{"result": "deleted"})
		}
	}

	return c.JSON(http.StatusNotFound, map[string]string{"error": "View has no such replica"})
}
