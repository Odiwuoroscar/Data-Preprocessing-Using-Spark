# Data-Preprocessing-Using-Spark
This project uses Apache Spark for distributed data preprocessing to maximize resource utilization and minimize data processing time.

The python script used starts by importing the necessary libraries and then it goes a head to handle missing values through imputation. Numerical missing values were filled using mean imputation, while categorical gaps were addressed with mode imputation. The data is then scrutinized for duplicate entries, removing any found to maintain data integrity. For the exploratory phase, the project employed various visualization techniques. Histograms were created to illustrate the distribution patterns of numerical predictors, while bar plots were utilized to showcase the frequency distribution of categorical variables. To identify potential outliers in numerical features, the code generated scatterplots.
The final step involved creating a correlation matrix. This allowed for the examination of relationships between various predictors and the response variable.

Key Steps:
Using 2 vms I generated ssh key pair for connection between the two vms (master and worker) using the following commands;

Ssh-keygen -t rsa

cat.ssh/id_rsa.pub >> ~/.ssh/authorized_keys

Spark configuration in the cluster mode.

Using tar spark package on hadoop1 and scp to hadoop2

On hadoop1

cd /opt

tar czf spark.tar.gz spark

scp spark.tar.gz root@hadoop2:/opt

On hadoop2

cd /opt

tar xvzf spark.tar.gz

To start spark

/opt/spark/sbin/start-all.sh

To submit a spark job I used Command

-- /opt/spark/bin/spark-submit --master spark://hadoop1:7077 /opt/spark_scrypt.py

Observations

It was observed that the duration for completing the action using only 1 Vm was approximately 2.0 minutes while the duration for completing a specific action using two VMs was about 8 seconds. Therefore running the Python code using both VMs (Hadoop 1 and Hadoop 2) demonstrates a significant improvement in processing speed.
