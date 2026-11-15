export const navigation = [
  [
    "Get started",
    [
      [
        "quickstart.md",
        "Quickstart",
        "Two folders, one toy model, no accounts. Ada still cannot read payroll."
      ],
      [
        "architecture.md",
        "How fyrr works",
        "A writer, a reader and a bucket walk into a bar. Nobody brings a database."
      ]
    ]
  ],
  [
    "Run it",
    [
      [
        "security.md",
        "Security model",
        "Strangers get no map. Not even a wrong one."
      ],
      [
        "http-users.md",
        "HTTP login and users",
        "No token, no harbour. Every request shows its papers."
      ],
      [
        "deployment.md",
        "Deployment",
        "Cron is a perfectly good captain."
      ],
      [
        "operations.md",
        "Operations",
        "Keep the lamp lit. Roll back when the sea turns."
      ]
    ]
  ],
  [
    "Reference",
    [
      [
        "configuration.md",
        "Configuration",
        "One TOML file. The secrets stay in the environment, where they belong."
      ],
      [
        "identity.md",
        "Identity and access",
        "Who is aboard, and who vouches for them."
      ],
      [
        "tuning.md",
        "Tuning",
        "Knobs for the curious. The defaults already sail."
      ],
      [
        "reader-writer.md",
        "Reader and writer",
        "Two crews, two sets of limits."
      ],
      [
        "cli.md",
        "CLI",
        "Everything fyrr --help says, with room to breathe."
      ],
      [
        "mcp-tools.md",
        "MCP tools",
        "Three tools. Your agent works out the rest."
      ]
    ]
  ],
  [
    "Connectors",
    [
      [
        "connectors/README.md",
        "Overview",
        "Six harbours, and a shipyard for your own."
      ],
      [
        "connectors/local-files.md",
        "Local files",
        "The humblest island: a folder."
      ],
      [
        "connectors/google-drive.md",
        "Google Drive",
        "Shared drives, and that one doc everybody can open by accident."
      ],
      [
        "connectors/notion.md",
        "Notion",
        "Pages, all the way down."
      ],
      [
        "connectors/github.md",
        "GitHub",
        "Issues, pull requests and the README nobody updates."
      ],
      [
        "connectors/slack.md",
        "Slack",
        "Where decisions go to be found again."
      ],
      [
        "connectors/jira.md",
        "Jira",
        "Tickets, comments and the epic from 2019."
      ],
      [
        "connectors/authoring.md",
        "Writing a provider",
        "Bring your own island."
      ]
    ]
  ]
] as const;
