# Databricks notebook source
# MAGIC %run "./ETL Workflows"

# COMMAND ----------

class WorkflowRunner:
    def __init__(self, name):
       self.name=name
   
    def runner(self):

       if self.name=="First Workflow":
           return FirstWorkFlow().runner()
       elif self.name=="Second Workflow":
           return SecondWorkFlow().runner()
       elif self.name=="Third Workflow":
           return ThirdWorkFlow().runner()
       elif self.name=="Fourth Workflow":
           return FourthWorkFlow().runner()
       else:
           raise ValueError(f"Not Implemented for name: {self.name}")


dbutils.widgets.dropdown(
name="workflow_list",
defaultValue="First Workflow",
choices=["First Workflow","Second Workflow","Third Workflow","Fourth Workflow"],
label="Which workflow to run")

wf=dbutils.widgets.get("workflow_list")

WorkflowRunner=WorkflowRunner(wf).runner()

# COMMAND ----------

