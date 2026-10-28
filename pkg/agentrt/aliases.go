package agentrt

import "github.com/urmzd/fiber/pkg/turn/native"

// Types, kept under their old names.
type (
	Access           = native.Access
	ApprovalRequest  = native.ApprovalRequest
	Approving        = native.Approving
	Awaiting         = native.Awaiting
	CompactRequest   = native.CompactRequest
	Compaction       = native.Compaction
	Granted          = native.Granted
	Message          = native.Message
	ModelNote        = native.ModelNote
	ModelRef         = native.ModelRef
	OpenAICompatible = native.OpenAICompatible
	Option           = native.Option
	Parked           = native.Parked
	PersonScoped     = native.PersonScoped
	ProviderFunc     = native.ProviderFunc
	Result           = native.Result
	Retriever        = native.Retriever
	Revision         = native.Revision
	Role             = native.Role
	Source           = native.Source
	Spec             = native.Spec
	Stop             = native.Stop
	ToolArguments    = native.ToolArguments
	ToolGroup        = native.ToolGroup
	ToolGroups       = native.ToolGroups
	Tools            = native.Tools
	ToolsFunc        = native.ToolsFunc
	Turn             = native.Turn
	UnavailableTool  = native.UnavailableTool
)

// Constants, kept under their old names.
const (
	AskPersonToolName       = native.AskPersonToolName
	DefaultAccounting       = native.DefaultAccounting
	DefaultKeepTurns        = native.DefaultKeepTurns
	DefaultMaxIter          = native.DefaultMaxIter
	DefaultOllamaHost       = native.DefaultOllamaHost
	DefaultOpenAIBaseURL    = native.DefaultOpenAIBaseURL
	DefaultQuestionTTL      = native.DefaultQuestionTTL
	DefaultStopReason       = native.DefaultStopReason
	MCPGroupPrefix          = native.MCPGroupPrefix
	MarkerApproval          = native.MarkerApproval
	MaxApprovalWait         = native.MaxApprovalWait
	MaxApprovers            = native.MaxApprovers
	MaxOptions              = native.MaxOptions
	MaxQuestionText         = native.MaxQuestionText
	TriggerRequested        = native.TriggerRequested
	PromptCacheOff          = native.PromptCacheOff
	PromptCacheShort        = native.PromptCacheShort
	PromptCacheLong         = native.PromptCacheLong
	RoleUser                = native.RoleUser
	RoleAssistant           = native.RoleAssistant
	ProviderAnthropic       = native.ProviderAnthropic
	ProviderOllama          = native.ProviderOllama
	ProviderOpenAI          = native.ProviderOpenAI
	ProviderOffline         = native.ProviderOffline
	EnvAnthropicKey         = native.EnvAnthropicKey
	EnvOllamaHost           = native.EnvOllamaHost
	EnvOpenAIKey            = native.EnvOpenAIKey
	EnvOpenAIBaseURL        = native.EnvOpenAIBaseURL
	FallbackOnAny           = native.FallbackOnAny
	FallbackOnUnavailable   = native.FallbackOnUnavailable
	FallbackOnRateLimit     = native.FallbackOnRateLimit
	FallbackOnTransient     = native.FallbackOnTransient
	FallbackOnAuth          = native.FallbackOnAuth
	FallbackOnContextLength = native.FallbackOnContextLength
	FallbackOnContentFilter = native.FallbackOnContentFilter
)

// Errors, kept under their old names.
var (
	ErrInvalid         = native.ErrInvalid
	ErrNoSuchTool      = native.ErrNoSuchTool
	ErrNoSuchToolGroup = native.ErrNoSuchToolGroup
)

// Functions, kept under their old names.
var (
	ActionTool           = native.ActionTool
	AnswerFor            = native.AnswerFor
	AskPersonTool        = native.AskPersonTool
	ConnectorTools       = native.ConnectorTools
	ConnectorToolsForRun = native.ConnectorToolsForRun
	DecodeMessage        = native.DecodeMessage
	EncodeMessage        = native.EncodeMessage
	FallbackConditions   = native.FallbackConditions
	IsToolGroup          = native.IsToolGroup
	MCPGroup             = native.MCPGroup
	MCPWriteGroup        = native.MCPWriteGroup
	RequestText          = native.RequestText
	ResolveTools         = native.ResolveTools
	StaticTools          = native.StaticTools
	WithAttachments      = native.WithAttachments
	WithCompaction       = native.WithCompaction
	WithLogger           = native.WithLogger
	WithMaxIter          = native.WithMaxIter
	WithOpenAICompatible = native.WithOpenAICompatible
	WithPromptCache      = native.WithPromptCache
	WithProvider         = native.WithProvider
	WithRetriever        = native.WithRetriever
	WithScript           = native.WithScript
	WithStrictTools      = native.WithStrictTools
	WithToolArguments    = native.WithToolArguments
	WithToolGroups       = native.WithToolGroups
	WithTools            = native.WithTools
)
