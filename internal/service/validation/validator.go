package validation

// Validator aggregates all entity validators
type Validator struct {
	User    *UserValidator
	Article *ArticleValidator
	Comment *CommentValidator
}

// NewValidator creates a new Validator with all entity validators
func NewValidator() *Validator {
	return &Validator{
		User:    NewUserValidator(),
		Article: NewArticleValidator(),
		Comment: NewCommentValidator(),
	}
}
