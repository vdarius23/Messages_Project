package schema

type Order struct {
	ID        string  `json:"id" bson:"_id,omitempty"`
	UserEmail string  `json:"user_email" bson:"user_email"`
	Amount    float64 `json:"amount" bson:"amount"`
	Status    string  `json:"status" bson:"status"`
}
