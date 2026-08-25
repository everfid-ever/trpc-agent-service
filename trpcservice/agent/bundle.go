package agent

import (
	"github.com/liuzengh/trpc-agent-service/trpcservice/profile"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	agentcore "trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/plugin"
	"trpc.group/trpc-go/trpc-agent-go/runner"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

type Bundle struct {
	AppName string
	Root    agentcore.Agent
	Plugins []plugin.Plugin
}

func (b *Bundle) NewRunner(sessions session.Service) (runner.Runner, error) {
	if b == nil || b.AppName == "" || b.Root == nil || sessions == nil {
		return nil, runtime.ErrCapabilityUnsupported
	}
	options := []runner.Option{runner.WithSessionService(sessions)}
	if len(b.Plugins) != 0 {
		options = append(options, runner.WithPlugins(b.Plugins...))
	}
	return runner.NewRunner(b.AppName, b.Root, options...), nil
}

var _ profile.RuntimeBundle = (*Bundle)(nil)
