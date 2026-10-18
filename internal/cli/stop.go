package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/spf13/cobra"
)

// newStopCmd builds `fiber stop`, which asks a run to end before it has
// finished.
//
// It prints that the request was made, never that the run stopped. The server
// cannot know which of the two happened when it answers, and neither can this
// command: the run's own stream says, and `fiber listen` reads it.
func newStopCmd() *cobra.Command {
	var reason string

	cmd := &cobra.Command{
		Use:   "stop <run-id>",
		Short: "Ask a run to stop",
		Long: "Ask a run to stop before it has finished.\n\n" +
			"The run wraps up its turn: the model call in flight is cancelled, what the\n" +
			"agent had said so far is kept, and the run ends with lifecycle.stopped. The\n" +
			"thread remembers the partial answer, so a reply continues from it.\n\n" +
			"The run id is the one `fiber tag --detach` prints and every event carries.\n" +
			"A run that has already ended, or does not exist, is left alone, and the\n" +
			"command still succeeds: a stop is a request, and the answer is the same for\n" +
			"every run id.",
		Example: "  fiber stop r0f3a...\n" +
			"  fiber stop r0f3a... --reason \"wrong repository\"",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			u, err := resolveUI(cmd)
			if err != nil {
				return err
			}
			cl, err := dial()
			if err != nil {
				return err
			}
			runID := strings.TrimSpace(args[0])
			if runID == "" {
				return usagef("stop: no run id")
			}
			if err := cl.stop(cmd.Context(), runID, reason); err != nil {
				return err
			}
			if u.format == FormatJSON {
				return u.json(map[string]any{"run_id": runID, "status": "requested"})
			}
			u.printf("%s\n", runID)
			u.logf("%s\n", u.dim("stop requested"))
			return nil
		},
	}
	cmd.Flags().StringVar(&reason, "reason", "", "Why the run is being stopped, shown where the run is rendered")
	return cmd
}

// stop posts a stop request for one run.
func (c *clients) stop(ctx context.Context, runID, reason string) error {
	body, err := json.Marshal(map[string]string{"reason": reason})
	if err != nil {
		return fmt.Errorf("cli: stop %s: %w", runID, err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		c.base+"/v1/runs/"+url.PathEscape(runID)+"/stop", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("cli: stop %s: %w", runID, err)
	}
	req.Header.Set("Content-Type", "application/json")
	res, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("%w: stop %s: %w", errUnavailable, runID, err)
	}
	defer func() { _ = res.Body.Close() }()
	if res.StatusCode != http.StatusAccepted {
		msg, _ := io.ReadAll(io.LimitReader(res.Body, 512))
		return fmt.Errorf("cli: stop %s: %s: %s", runID, res.Status, strings.TrimSpace(string(msg)))
	}
	return nil
}
