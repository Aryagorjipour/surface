package connector

import "net/http"

func StepFrom(status, action string) (next string, code int, write bool) {
	switch action {
	case "accept":
		if status == "pending" {
			return "accepted", http.StatusOK, true
		}
	case "reject":
		if status == "pending" {
			return "rejected", http.StatusOK, true
		}
	}
	if status == "accepted" || status == "rejected" {
		return status, http.StatusConflict, false
	}
	return status, http.StatusConflict, false
}
