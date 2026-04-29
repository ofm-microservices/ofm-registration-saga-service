package nats

type userCreateResult struct {
	SessionID string `json:"session_id"`
	UserID    string `json:"user_id"`
	Status    string `json:"status"`
	Error     string `json:"error,omitempty"`
	Timestamp string `json:"timestamp"`
}

type authCreatePendingResult struct {
	SessionID string `json:"session_id"`
	ClientID  string `json:"client_id"`
	UserID    string `json:"user_id"`
	Status    string `json:"status"`
	Error     string `json:"error,omitempty"`
	Timestamp string `json:"timestamp"`
}

type mailSendResult struct {
	SessionID     string `json:"session_id"`
	ClientID      string `json:"client_id"`
	UserID        string `json:"user_id"`
	RequestID     string `json:"request_id,omitempty"`
	CorrelationID string `json:"correlation_id,omitempty"`
	MessageType   string `json:"message_type"`
	To            string `json:"to"`
	Status        string `json:"status"`
	Error         string `json:"error,omitempty"`
	Timestamp     string `json:"timestamp"`
}
