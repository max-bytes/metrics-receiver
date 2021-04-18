package enrichments

type EfectiveTraitsResponse struct {
	Traits map[string]Trait
}

type Trait struct {
	TraitAttributes map[string]TraitAttribute `json:"traitAttributes"`
}

type TraitAttribute struct {
	Id    string         `json:"id"`
	Name  string         `json:"name"`
	Value AttributeValue `json:"value"`
	CIId  string         `json:"ciid"`
	State string         `json:"state"`
}

type AttributeValue struct {
	Type    string   `json:"type"`
	IsArray bool     `json:"isArray"`
	Values  []string `json:"values"`
}
