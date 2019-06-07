# Redshift data warehouse set-up and ETL

This is an educational practice project to grasp the funadamentals of cloud data warehouses (DWH), Amazon Redshift, in particular. The exercise covers the following key areas:

- Infrastructure-as-Code: practice of setting up and managing cloud resources via code, which affords a great deal of documentability, understandability and reproducibility
- Design of a star schema for the Redshift data warehouse
- Copying at scale data residing in an Amazon S3 storage directly to tables (staging, temporary ones) in Redshift
- ETL of data within Redshift from staging to final star schema tables  

The project is based on a hypothetical case of a music streaming app start-up, which currently keeps all the logs of its clients' sessions in daily JSON files in S3. Hence, the objective of the project is to migrate the data into an appropriately designed DWH in Redshift and enable analytics and business intelligence applications.

## Database design considerations

In Cassandra, data modeling is led by the understanding of the types of queries one expects to be performing on the database. Denormalization and redundancy are inherent to Cassandra databases, for the benefit of query efficiencies on big data volumes. Moreover, any attributes included in the WHERE statements have to be part of the table's primary key. 


## Requirements

- Cassandra database server
- Python 3 version
- Python `cassandra` database API
- Python `os, glob, csv and pandas` packages
- Jupyter Notebook 


## Running the code

As the first step, set up a Redshift cluster, with the relevant role and security configurations (external access through port 5439 should be enabled). You can do this by running the code in the Jupyter Notebook file, `IaC.ipynb`. **You should include your own access key ID and secret access key in the configuration `dwh.cfg` file.** To get these credentials, you first need to create a user. Please refer to this [documentation](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_users_create.html) on the various options for doing this. You should also include the Redshift role ARN (Amazon Resource Name) in the configuration file, after you've created it and retrieved its name per the code in the notebook. Feel free to change cluster and database names, passwords, etc, in the configuration file. For this project, I recommend you stick to the defined cluster and node types and numbers. **Once you are finished with the project, make sure you run the cluster deletion code at the end of the notebook, to avoid incurring Redshift charges.**

After you have set up the cluster and the configuration file per the instructions above, go the the folder where the configuraiton file resides and run the `create_tables.py` and `etl.py` codes. Note that the latter code (ETL) can take a few minutes to complete. Per the set-up of this project, the Redshift cluster and the S3 storage are in the same region (us-west-2), which helps the timing significantly. In my case, the ETL took only a couple of minutes. I also tested the timing with the two resources being placed in different regions (eu-west-1 and us-west-2), in which case the time went up to 40 minutes!

You can then run test analytic queries either directly in the Redshift query editor or in the section provided in the Jupyter notebook. 