# Redshift data warehouse set-up and ETL

This is an educational practice project to grasp the funadamentals of cloud data warehouses (DWH), Amazon Redshift, in particular. The exercise covers the following key areas:

- Infrastructure-as-Code: practice of setting up and managing cloud resources via code, which affords a great deal of documentability, understandability and reproducibility
- Design of a star schema for the Redshift data warehouse
- Copying at scale data residing in an Amazon S3 storage directly to staging (temporary) tables in Redshift
- ETL of data within Redshift from staging to the final star schema tables  

The project is based on a hypothetical case of a music streaming app start-up, which currently keeps all the logs of its clients' sessions in daily JSON files in S3. Hence, the objective of the project is to migrate the data into an appropriately designed DWH in Redshift and enable analytics and business intelligence applications.

## Data warehouse design

This DWH is based on a classic star schema design, with one fact table (`songplays`) and four dimension tables (`users`, `songs`, `artists`, `time`). Two staging tables were also defined to enable bulk copying of data directly from S3 and further ETL into the final tables. 

The star schema allows running various analytical / business intelligence queries such as the one below:

```
%%sql
SELECT title
FROM songplays sp
JOIN time t ON sp.start_time_ms=t.start_time_ms
JOIN songs s ON sp.song_id=s.song_id
WHERE hour=2;

 * postgresql://georgen:***@udacitycluster.ct1vzfrevpbv.us-west-2.redshift.amazonaws.com:5439/sparkify
3 rows affected.

title
Make Her Say
If I Ain't Got You
I CAN'T GET STARTED
```

## Requirements

- Python 3 version
- Jupyter Notebook
- Python [Boto3](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/quickstart.html) library for AWS python SDK


## Running the code

As the first step, set up a Redshift cluster, with the relevant role and security configurations (external access through port 5439 should be enabled). You can do this by running the code in the Jupyter Notebook file, `IaC.ipynb`. **You should include your own access key ID and secret access key in the configuration `dwh.cfg` file.** To get these credentials, you first need to create a user. Please refer to this [documentation](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_users_create.html) on the options for doing this. Later, you should also include in the configuration file the Redshift role ARN (Amazon Resource Name), and the cluster host endpoint. You can do so after you've created the role and the cluster and retrieved these items per the code in the notebook. 

Feel free to change the cluster and database names, passwords, etc, in the configuration file.

After you have set up the cluster and the configuration file per the instructions above, go the the folder where the configuraiton file resides and run the `create_tables.py` and `etl.py` codes. Note that the latter code (ETL) can take a few minutes to complete. Per the set-up of this project, the Redshift cluster and the S3 storage are in the same region (us-west-2), which helps the timing significantly. In my case, the ETL took only a couple of minutes. I also tested the timing with the two resources being placed in different regions (eu-west-1 and us-west-2), in which case the time went up to 40 minutes!

After the above steps have been completed, you can run test analytic queries either directly in the Redshift query editor or in the section provided in the Jupyter notebook. 

**Once you are finished with the project, make sure you run the cluster deletion code at the end of the notebook, to avoid incurring Redshift charges.**