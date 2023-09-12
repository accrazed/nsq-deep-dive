package src

type Message struct {
	Topic   string
	Payload MessagePayload
}

type MessagePayload struct {
	Status int    `json:"status"`
	Body   string `json:"body"`
}
