package api

import (
	"net/http"

	"github.com/eventlineage/backend/internal/store"
	"github.com/gin-gonic/gin"
)

// MessageHandler handles message-related API endpoints.
type MessageHandler struct {
	messageRepo *store.MessageRepository
}

// NewMessageHandler creates a new message handler.
func NewMessageHandler(messageRepo *store.MessageRepository) *MessageHandler {
	return &MessageHandler{messageRepo: messageRepo}
}

// GetMessage returns a single message with its relationships.
// @Summary Get message
// @Description Get a message by ID with parent and children
// @Tags messages
// @Accept json
// @Produce json
// @Param messageId path string true "Message ID"
// @Success 200 {object} models.MessageWithRelations
// @Failure 404 {object} ErrorResponse
// @Failure 500 {object} ErrorResponse
// @Router /api/v1/messages/{messageId} [get]
func (h *MessageHandler) GetMessage(c *gin.Context) {
	messageID := c.Param("messageId")
	if messageID == "" {
		respondError(c, http.StatusBadRequest, "messageId is required")
		return
	}

	result, err := h.messageRepo.GetMessageWithRelations(c.Request.Context(), messageID)
	if err != nil {
		respondError(c, http.StatusNotFound, err.Error())
		return
	}

	respondSuccess(c, result)
}
