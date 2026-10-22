# Serverless B2B Data Enrichment Pipeline

An automated, serverless data pipeline designed to enrich B2B company data at scale. This architecture processes raw company domains and outputs structured, classified intelligence using a multi-pass extraction methodology.

## System Architecture

*   **Cloud Orchestration (Modal):** Built on Modal to enable asynchronous, serverless horizontal scaling. The `cloud_orchestrator` distributes concurrent data processing tasks across multiple ephemeral worker containers.
*   **Enterprise Security (Microsoft Entra ID):** The web dashboard interface is secured via OAuth2 Single Sign-On (SSO) middleware, ensuring strict, session-based access control for enterprise users.
*   **Two-Pass Extraction Engine:**
    *   **Phase 1 (Data Crawling):** Utilizes the Bright Data API for initial baseline data extraction (e.g., headcount, founding year) and the Exa API for deep web contextual fact-sheet generation.
    *   **Phase 2 (LLM Classification):** Leverages large language models (DeepSeek-v4-pro) with strict JSON schema enforcement to evaluate business mechanics, followed by routing through a Jev model (via OpenRouter) to map outputs to a complex vertical taxonomy.

## Technical Highlights

*   **FastAPI / ASGI:** Integrates a lightweight FastAPI web application for users to upload datasets and trigger background enrichment jobs.
*   **Resilient Error Handling:** Includes API timeout fallbacks, rate-limit backoffs, and strict 'Other' gating logic to prevent pipeline hallucination on out-of-scope targets.
*   **Data Integrity:** Outputs clean, normalized Excel artifacts directly into secure Cloud Volumes.

*Note: Proprietary classification taxonomies, internal schemas, and specific competitor watchlists have been sanitized or generalized in this public repository for confidentiality.*
