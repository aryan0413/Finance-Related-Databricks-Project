
# Finance-Related-Databricks-Project #
Overview
This repository contains the full Banking (NEOBANK) data engineering and analytics project developed on Databricks. It is structured for reproducibility, collaboration, and extensibility using Databricks Repos and integrates with GitHub for version control.

Contents
Banking/
The main project folder, organized into:

01_Setup_Metadata/ – Notebooks and scripts for workspace and configuration metadata setup.
02_Source_to_Silver/ – Data ingestion scripts and notebooks for loading source data and transforming it into Silver tables.
03_Silver_to_Gold/ – Transformation logic for curating Gold business tables and aggregates.
04_Email_Notification/ – Automated notification notebooks (typically handles alerts/emails after pipeline runs).
05_Dashboard/ – Artifacts for NeoBank dashboards and visualization assets.
(Add other subfolders/files here as needed, such as pipeline definitions, config files, or jobs.)

## How This Project Was Integrated ##
Developed in Databricks:
All data pipelines, transformations, and analytics scripts were authored in Databricks notebooks and jobs, following a multi-layered medallion architecture (Bronze → Silver → Gold).

## Version Control with GitHub: ##
The entire Banking project folder was moved under Databricks Repos and committed/pushed to this GitHub repository using Databricks’ GitHub integration (via OAuth and the Databricks GitHub App).

## Syncing Databricks & GitHub: ##
All future edits to code, notebooks, or configs should be done via Databricks Repos (not Workspace), and committed/pushed using the Git pane—keeping this repo up-to-date and in sync.

## How to Use ##
Clone Repo
In Databricks, open the Repos pane and clone this repository directly using:
https://github.com/aryan0413/Finance-Related-Databricks-Project

This will create a managed Git folder and let you make changes in notebooks or files.
Run Notebooks or Pipelines

Explore the /Banking/ subfolders for pipeline entry points (setup, ingestion, transformation, dashboard).
Open and run notebooks as needed. Adapt configurations to your Databricks workspace if required.
Commit & Push Changes

Use the Databricks Git integration controls to stage, commit, and push your updates.
Always keep your code changes under version control for reproducibility.
Re-create Jobs or Pipelines

Jobs are not stored directly, but all required code and configuration for pipelines/jobs are captured in scripts or notebooks.
Use notebook paths and pipeline scripts to re-define Databricks Jobs as needed in your workspace.
Project Highlights
Implements a modern medallion architecture for financial data pipelines
Modular notebook organization: setup, ingestion (Silver), transformation (Gold), and notification layers
Ready for extension: add new data sources, analytics, or dashboards by branching from this structure
Best Practices
Always modify assets (notebooks/scripts) from the Databricks Repos view—not the workspace browser—to ensure full Git/GitHub tracking.
Commit and push regularly to capture all changes.
To add new features, use a branch/PR workflow if collaborating.
