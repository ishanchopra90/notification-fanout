Event-Driven Notification Fanout Service

Coding Interview: Event-Driven Notification Fanout REST API Service
Mandatory Environment Rules
To ensure your work is saved and evaluated correctly, you must adhere to the following operational constraints:

Stay in the Workspace: All development must occur inside the /workspaces directory within your IDE.
No Host Saving: Do not save code or assets to the host machine's Desktop or Downloads folder. These areas are not bridged to the container; data saved there will be permanently lost when the session ends.
Final Submission: You must push your code and architecture diagram to a personal GitHub repository before the timer expires.
Session Cleanup: Sign out of all personal accounts (GitHub, Cursor, Browser) before handing back the workstation.
Logistics and Tooling
Item

Details

Time Limit

3 Hours

Supported Languages

GoLang (Required). Please note that while Python, Java, and TypeScript / Node.js are also installed on the container environment, you must use Go for this assessment. (Standard extensions for YAML and Go are highly recommended).

IDE Environment

VS Code or Cursor, bridged directly to a Ubuntu 24 Dockerized container.

AI Assistance

GitHub Copilot, Claude Code, and Cursor are permitted. You are encouraged to use Chrome, documentation, and any AI tools to assist your development. You must sign in using your personal accounts and remain fully responsible for the review, architecture, and correctness of all generated outputs.

Permissions

You have passwordless sudo access to install additional dependencies via apt or brew.

Pre-Installed Utilities

GitHub CLI (gh), doctl, s3cmd, jq, yq, neovim, and core image processing libraries.

Project Objective
Summary: Build a production-ready event ingestion and notification fanout service that accepts structured events, matches them against subscriber rules, and delivers notifications to registered webhook endpoints with delivery guarantees and observability.

Note: MUST USE DOKS to deploy the solutions.

Functional Expectations
At a minimum, your service should demonstrate:

Event Ingestion: Accept structured events over a REST endpoint (or a queue of your choice). Each event has a type, source, and arbitrary JSON payload. Ingestion should be durable — accepted events must not be silently dropped.
Subscription Management: Allow subscribers to register webhook URLs with filter rules (event type, source, payload conditions). Support creating, listing, and deleting subscriptions without restart.
Fanout & Delivery: For each accepted event, evaluate all subscriptions and deliver matching events to subscriber webhooks. Deliveries must be retried with back-off on failure. Delivery state (pending, delivered, failed) must be queryable.
Delivery Audit: Expose an endpoint returning delivery history per event or per subscription — including attempt timestamps, HTTP response codes received, and final status.
Engineering Expectations
Your solution should reflect what you believe constitutes a production-ready service. We require:

Architecture Flow Diagram: Map the full path from event ingest through subscriber matching, fanout, retry loop, and delivery audit store.
Delivery Guarantees: Document whether your design is at-least-once, at-most-once, or exactly-once for a given subscriber, and what the failure conditions are for each.
Testing: Cover all scenarios with unit and integration tests.
CI/CD: A functional GitHub Actions pipeline that runs your test suite on push.
Documentation: README covering setup, filter rule syntax, and a section on 'what we sacrifice for simplicity vs. what we'd harden next'.
Extensions & Next Steps
If time permits, you are encouraged to expand on your solution:

Deployment: Deploy to DigitalOcean; use a real managed message queue (e.g. DO Managed Kafka or Redis Streams) as the fanout backbone.
Ordering Guarantees: Add support for per-source ordered delivery to a single subscriber.
Replay API: Allow a subscriber to request re-delivery of events from a time range, useful after a subscriber-side outage.
Start in VS Code →
