# HLS SQL Workshop on Databricks

The repository is can be used for setting up the Healthcare & Life Sciences SQL Workshop on Databricks.

We suggest executing any of the workflows and DLT using Serverless compute. If Serverless compute is not available, we suggest using the Databricks Runtime **14.3 LTS** or higher. 

## [Summary](#summary)

This guide explores how Databricks' Data Intelligence Platform transforms modern data operations with its powerful Data Warehousing capabilities. It demonstrates ingesting claims data from CMS using Databricks Workflows, highlighting the platform’s scalability, flexibility, and cost efficiency for large-scale data processing.

Key features include:
  - **Databricks SQL**: Databricks SQL is the intelligent data warehouse. Built with DatabricksIQ, the Data Intelligence Engine that understands the uniqueness of your data, Databricks SQL democratizes analytics for technical and business users alik
  - **Unity Catalog**: A unified governance solution for data and AI assets on Databricks that provides centralized access control, auditing, lineage, and data discovery capabilities across Databricks.
  - **DatabricksIQ**: Data Intelligence Engine that uses AI to power all parts of the Databricks Data Intelligence Platform. It uses signals across your entire Databricks environment, including Unity Catalog, dashboards, notebooks, data pipelines and documentation to create highly specialized and accurate generative AI models that understand your data, your usage patterns and your business terminology.
  - **AI/BI Dashboards**: Easy-to-create, shareable dashboards built on governed data to drive informed decision-making across teams.
  - **Genie**: Natural language interaction with data, simplifying access and democratizing data usage within organizations.

The workshop showcases how Databricks enables efficient, governed, and accessible data management for enterprise-grade solutions.

## [Prerequisites](#prerequisites)
In order to properly setup and successfully run the workshop, there are several prerequisites:
- The code base is implemented to execute successfully ONLY on the Databricks platform (GCP, AZURE, OR AWS).
- Databricks E2 Deployment
- [**Workspace for the Workshop**](#workspace-for-the-workshop)
<br>There are several preferred options for running this workshop. Please work with your account team to decide which method is best for your customer
  - Run the workshop in a CloudLabs environment.
  - Run the workshop in a Databricks Express environment.
  - Run the workshop in a Customer workspace.
- Databricks Repos functionality.
- Access to GitHub via public internet access.
- Access to Serverless compute or compute using Databricks Runtime **14.3 LTS**
- Unity Catalog enabled.  

# [Workshop Setup](#workshop-setup)
Follow the steps below to setup your environment for the workshop.

## [Setting up Git Integration in your workspace](#setting-up-git-integration-in-your-workspace)
> ### STEP 1: Fork Github repo (optional)
In your github repository, please fork the main repo, so that you could work with the forked repo when required. Here is how you could fork the repo in Github. 

![](/python_deploy/readme_images/create_git_folder.png "Create git folder")

> ### STEP 2: Create a Git folder in the workspace
In your workspace, navigate to **Workspace -> Home-> Create->Git** Folder as below. 

![](/python_deploy/readme_images/create_git_folder_pt2.png "Create git folder")

Once you click Repo, you will be directed to the screen below, simply input the Git repository URL (example link), and click Create Repo. 

![](/python_deploy/readme_images/fork_repo.png "Fork Repo")

Once the repo is created, navigate to the repo **hls_sql_workshop->python_deploy** and click on the notebook **hls_sql_workshop_driver**. 

![](/python_deploy/readme_images/git_execution.png "")

You will be navigated to the the notebook contents below: 

![](/python_deploy/readme_images/notebook_execution.png "")

> ### STEP 3: Execute the notebook

Run the notebook cell to declare the widgets and assign variables, you will have a list of widgets available for you to set up depending on what type of workflows you prefer. It supports the catalog, schema, and volume that will be created, along with if you want to use classic or serverless compute within your workflow and DLT pipeline. It is recommended to use serverless compute unless your workspace requires classic compute. 

![](/python_deploy/readme_images/widgets.png "")

Next, run the entirety of the notebook, which will generate the necessary  DLT pipeline and workflow with the required tasks for setup.

Here is an example of results based on example settings previously. 

![](/python_deploy/readme_images/workflow_output.png "")

Once the workflow is created, you are ready to go to the generated workflow to run it and complete setup.
## [Navigating Databricks Workflow](#navigating-databricks-workflow)
Click on the generated task link to guide you to the workflow details below. 

Alternatively, from the Databricks homepage, navigate to the  persona menu in the top left, click on Workflows and you will see a list of the workflows that were created by yourself. Click on the specific workflow will navigate you to the workflow details page. 

![](/python_deploy/readme_images/workflow.png "")

> ### STEP 1: Databricks Workflow

Click on Tasks in your workflow, you will see all the tasks for the workflow, feel free to click on individual tasks and see what’s the settings for the particular task. 

![](/python_deploy/readme_images/workflow_tasks.png "")

> ### STEP 2: Confirm Compute

Confirm that your workflow was setup with the proper compute for your workspace. As always, serverless is always strongly recommended if your workspace is enabled for serverless.

![](/python_deploy/readme_images/workflow_compute.png "")

If you selected classic compute during setup, you will see the job cluster specs that also includes the node type.

![](/python_deploy/readme_images/workflow_compute_pt_2.png "")

## [Run the Workflow](#run-the-workflow)
Once you have confirmed the settings, run the workflow to complete setup. The entire workflow will take ~1 hour to complete. The serving endpoint created at the end of the workflow will take some time to be available once the workflow completes.

## [Confirm SQL Warehouse](#confirm-sql-warehouse)
Once the workflow has completed successfully, confirm that the Serverless SQL Warehouse was created. If it was not created, please use an appropriate existing SQL Warehouse or create a new one manually. The selected SQL Warehouse should be the one used during the workshop.

> ### STEP 1: Confirm SQL Warehouse

Navigate to **SQL Warehouses** and confirm that the serverless SQL Warehouse was created: **SQL_Workshop_Serverless_Warehouse**.

The workshop will attempt to create the serverless warehouse during its execution. If it is unable to create it, the task will fail but the workflow will continue to execute.

The notebook to create the SQL Warehouse will only attempt to create a serverless warehouse (it will not attempt to create a SQL Warehouse with PRO compute), and the name will be **SQL_Workshop_Serverless_Warehouse**. You can manually adjust the settings (e.g. size, active/max) if needed.

> ### STEP 2: Create new SQL Warehouse manually (if needed)

If the SQL Warehouse was not created automatically during setup, manually create a new one. Below are the recommended configurations:
  - **Name**: SQL_Workshop_Serverless_Warehouse
  - **Size**: Small
  - **Cluster Count**: 
    - Min: 1
    - Max: 10

Please note, if you are required to use Pro instead of serverless, please **adjust the min cluster count to a higher number** to support seamless concurrency for users. Also, please adjust this and the max clusters accordingly based on the number of workshop participants. 

## [Confirm UC Objects](#confirm-uc-objects)
Once the workflow has completed successfully, confirm that the UC objects were all setup based on your catalog and schema widget inputs.

> ### STEP 1: Confirm Catalog and Schemas
Confirm the catalog and schemas were created. You should see the following schemas in your catalog:
- ai
- \<name of your schema> (default value is cms)
- gold

![](/python_deploy/readme_images/uc_objects.png "")
 

> ### STEP 2: Confirm Tables, Volumes, and Models
In the schema you created (default value is cms) you should see 23 bronze/silver/gold tables were created and 1 volume. The volume (default value is raw_files) should show the cms files that were created in the root directory called medicare_claims.

Tables created:

![](/python_deploy/readme_images/uc_tables.png "")

CMS data located in the volume in the root directory _medicare_claims_:

![](/python_deploy/readme_images/uc_volumes.png "")

In the gold schema you should see the star schema that was created with dim and fact tables. These tables have primary key and foreign key constraints on them, which are required for the assistant and Genie portions of the demo (DLT currently does not allow for PK and FK constraints, therefore these tables were copied from the DLT gold tables).

![](/python_deploy/readme_images/pk_fk.png "")

In the ai schema, you should see 3 tables and a registered model:

![](/python_deploy/readme_images/ml_model.png "")

> ### STEP 3: Confirm Serving Endpoint
Go to **Serving** and search for _predict_claims_amount_ and confirm that the serving endpoint was created. The serving endpoint may take some time until it’s Serving endpoint state it's _Serving endpoint state is ready_

![](/python_deploy/readme_images/serving_endpoint.png "")

## [Create Genie Space](#create-genie-space)
At the time of writing, Genie Rooms are not able to be created programmatically, therefore you will need to create the Genie Space manually.

> ### STEP 1: Create Genie Space

Go to **Genie -> New** and create the Genie Space using the setting below: 
**Title:** CMS Genie Space
**Description:**
**Default Warehouse: **SQL_Warehouse_Serverless_Warehouse**
This was the warehouse created by the workflow during setup. If it was not created during setup, please select the name of the warehouse you created manually in previous steps.
**Tables:** Select all tables from the gold schema
- dim_beneficiary
- dim_date
- dim_diagnosis
- dim_provider
- fact_carrier_claims
- fact_patient_claims
- fact_prescription_drug_events

**Sample Questions:**
- What is the total number of claims submitted in a given year?
- Who are the top 5 beneficiaries by claims submitted?

