package api

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/google/uuid"

	"github.com/kaancolakoglu/NotificationSystem/internal/store"
	"github.com/kaancolakoglu/NotificationSystem/internal/validate"
)

var placeholder = regexp.MustCompile(`\{\{([A-Za-z0-9_]+)\}\}`)

func (s *server) CreateTemplate(ctx context.Context, request CreateTemplateRequestObject) (CreateTemplateResponseObject, error) {
	if err := checkTemplate(request.Body.Content); err != nil {
		return CreateTemplate422JSONResponse{Message: err.Error()}, nil
	}
	id, err := uuid.NewV7()
	if err != nil {
		return nil, fmt.Errorf("generate id: %w", err)
	}
	row, err := s.queries.InsertTemplate(ctx, store.InsertTemplateParams{ID: id, Content: request.Body.Content})
	if err != nil {
		return nil, fmt.Errorf("insert template: %w", err)
	}
	return CreateTemplate201JSONResponse{Id: row.ID, Content: row.Content, CreatedAt: row.CreatedAt}, nil
}

func checkTemplate(content string) error {
	if content == "" {
		return errors.New("content must not be empty")
	}
	if strings.ContainsRune(content, 0) {
		return errors.New("content must not contain a NUL character")
	}
	rest := placeholder.ReplaceAllString(content, "")
	if strings.Contains(rest, "{{") || strings.Contains(rest, "}}") {
		return errors.New("content has double braces that are not a placeholder: write {{name}}, with letters, digits and underscores only")
	}
	return nil
}

func (s *server) templatesOf(ctx context.Context, items []CreateNotificationRequest) (map[uuid.UUID]string, error) {
	var ids []uuid.UUID
	for _, item := range items {
		if item.TemplateId != nil {
			ids = append(ids, *item.TemplateId)
		}
	}
	if len(ids) == 0 {
		return nil, nil
	}
	rows, err := s.queries.GetTemplates(ctx, ids)
	if err != nil {
		return nil, fmt.Errorf("get templates: %w", err)
	}
	templates := make(map[uuid.UUID]string, len(rows))
	for _, row := range rows {
		templates[row.ID] = row.Content
	}
	return templates, nil
}

func contentOf(item CreateNotificationRequest, templates map[uuid.UUID]string) (string, error) {
	if item.TemplateId == nil {
		if len(item.Variables) > 0 {
			return "", errors.New("variables need a template_id")
		}
		return item.Content, nil
	}
	if item.Content != "" {
		return "", errors.New("send content or template_id, not both")
	}
	template, found := templates[*item.TemplateId]
	if !found {
		return "", errors.New("template not found")
	}
	return render(template, item.Variables)
}

func render(template string, variables map[string]*string) (string, error) {
	var rendered strings.Builder
	var missing []string
	seen := map[string]bool{}
	copied := 0
	for _, match := range placeholder.FindAllStringSubmatchIndex(template, -1) {
		name := template[match[2]:match[3]]
		value := variables[name]
		if value == nil && !seen[name] {
			missing = append(missing, name)
		}
		seen[name] = true
		rendered.WriteString(template[copied:match[0]])
		if value != nil {
			rendered.WriteString(*value)
		}
		copied = match[1]
		if rendered.Len() > validate.EmailMaxContentBytes {
			return "", fmt.Errorf("rendered content must be at most %d bytes", validate.EmailMaxContentBytes)
		}
	}
	rendered.WriteString(template[copied:])
	if len(missing) > 0 {
		return "", fmt.Errorf("variables are missing: %s", strings.Join(missing, ", "))
	}
	return rendered.String(), nil
}
