package cli

import (
	"strings"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"

	mandatumv1 "github.com/urmzd/mandatum/gen/mandatum/v1"
	"github.com/urmzd/mandatum/pkg/agentrt/payload"
)

// newAnswerCmd builds `mandatum answer`, which answers a question a run is
// parked on.
//
// It prints the answer the question has, which is this one unless somebody
// answered first: a question takes one answer, and a second is told the first.
func newAnswerCmd() *cobra.Command {
	var (
		approve, deny bool
		args          string
		choose        string
		reply         string
		note          string
	)
	cmd := &cobra.Command{
		Use:   "answer <run-id> <question-id>",
		Short: "Answer a question a run is waiting on",
		Long: "Answer a question a run is parked on, and wake the run.\n\n" +
			"A run parks when a tool call needs approval or the agent asks a person\n" +
			"something with ask_person. lifecycle.parked carries the question and its id\n" +
			"(`mandatum listen` prints them). An approval takes --approve or --deny, and\n" +
			"--args to approve the call with edited arguments; a choice takes --choose\n" +
			"with one of the options offered; a reply takes --reply.\n\n" +
			"The first answer recorded is the answer. A second one is told what it was.",
		Example: "  mandatum answer r0f3a... int_9c1e... --approve\n" +
			"  mandatum answer r0f3a... int_9c1e... --deny --note \"freeze week\"\n" +
			"  mandatum answer r0f3a... int_9c1e... --approve --args '{\"env\":\"staging\"}'\n" +
			"  mandatum answer r0f3a... int_4b2d... --choose staging\n" +
			"  mandatum answer r0f3a... int_77aa... --reply \"the fix is urgent\"",
		Args: cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, argv []string) error {
			u, err := resolveUI(cmd)
			if err != nil {
				return err
			}
			a := &mandatumv1.Answer{Note: note}
			given := 0
			for _, set := range []bool{approve, deny, choose != "", reply != ""} {
				if set {
					given++
				}
			}
			switch {
			case given != 1:
				return usagef("answer: give exactly one of --approve, --deny, --choose and --reply")
			case args != "" && !approve:
				return usagef("answer: --args goes with --approve")
			case approve:
				a.Decision, a.ArgumentsJson = payload.DecisionApprove, args
			case deny:
				a.Decision = payload.DecisionDeny
			case choose != "":
				a.Choice = choose
			default:
				a.Text = reply
			}
			cl, err := dial()
			if err != nil {
				return err
			}
			res, err := cl.invoke.Answer(cmd.Context(), connect.NewRequest(&mandatumv1.AnswerRequest{
				RunId: strings.TrimSpace(argv[0]), QuestionId: strings.TrimSpace(argv[1]), Answer: a,
			}))
			if err != nil {
				return connectErr("answer", err)
			}
			got := res.Msg.GetAnswer()
			if u.format == FormatJSON {
				return u.json(map[string]any{
					"run_id": argv[0], "question_id": got.GetQuestionId(), "first": res.Msg.GetFirst(),
					"decision": got.GetDecision(), "arguments": got.GetArgumentsJson(), "choice": got.GetChoice(),
					"text": got.GetText(), "note": got.GetNote(), "by": got.GetBy(), "expired": got.GetExpired(),
				})
			}
			u.printf("%s\n", firstNonEmptyString(got.GetDecision(), got.GetChoice(), got.GetText()))
			switch {
			case got.GetExpired():
				u.logf("%s\n", u.dim("the question had expired; its default was recorded"))
			case !res.Msg.GetFirst():
				u.logf("%s\n", u.dim("already answered by "+got.GetBy()))
			}
			return nil
		},
	}
	f := cmd.Flags()
	f.BoolVar(&approve, "approve", false, "Approve the tool call")
	f.BoolVar(&deny, "deny", false, "Deny the tool call")
	f.StringVar(&args, "args", "", "With --approve: the arguments to run the call with, as a JSON object")
	f.StringVar(&choose, "choose", "", "Choose one of the options offered")
	f.StringVar(&reply, "reply", "", "Reply in your own words")
	f.StringVar(&note, "note", "", "A note for the record; a denial's note is shown to the agent")
	return cmd
}
