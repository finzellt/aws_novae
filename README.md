# Open Nova Catalog (2nd Gen)

## Introduction
Open Nova Catalog (2nd Gen; ONC2) is a project to aggregate data for Classical Novae. A spiritual successor to the original Open Nova Catalog ([ONC](https://github.com/astrocatalogs/novae)), which was built using the [AstroCats framework](https://github.com/astrocatalogs), it's purpose is to make data easily accessible for all.

Built using Amazon Web Services (AWS), the project currently consists of a pipeline for robustly finding metadata and (potential) data sources for a given nova. Future features will include both data harvesting and a website/API for the public to view and and download the data.

Currently, the project uses an AWS Step Function to run four (AWS) Lambda functions, all of which live in the `nova-ingest` directory:
- **Resolve_Simbad_Metadata**: Qeries Simbad to extract metadata
- **Determine_Host_Galaxy**: Determine the galaxy of the nova.
- **Query_Ads_Bibcodes**: Queries ADS for all bibliographic sources related to the nova. 
- **Stage_Write_Metadata**: Writes out both a metadata file and queues--in a DynamoDB table--bibliographic sources that may have associated data.


## Using ONC2

The project is not currently in a state for public use. Check back later for updates.

<!-- research and educational project for studying classical novae and their environments, with a focus on integrating computational methods into astronomy. The repository contains scripts, data-processing tools, and modeling workflows used to identify, analyze, and visualize novae and their host stellar systems.

Currently, the repository centers on V1324 Sco, a well-studied nova that serves as a test case for the pipeline. -->