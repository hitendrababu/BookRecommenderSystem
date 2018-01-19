# BookRecommenderSystem
Project Link:  https://sites.google.com/view/bookspot/home




Instructions: 

We have used AWS cluster to run our application. We are using two different approaches for this project
    1. User based Recommendation ( user.py )
    2. Item based Recommendation ( item.py )

First download the project dataset from the following website: http://www2.informatik.uni-freiburg.de/~cziegler/BX/
Then we will obtain 3 files BX-Books.csv, BX-Book-Ratings.csv and BX-Users.csv.
We have to pre-process the data in the dataset so that we won't encounter any illegal format while reading the data.

Now we will use the BX-Book-Ratings.csv data which has around 1.48 millions records for both our programs.

AWS cluster:

To run a program in AWS cluster , we have to initially create a cluster in AWS. To create a cluster in AWS, there 
are few prerequisites.

1. Create a bucket in Amazon S3, then upload the data files in it and save it.
2. Create a Amazon KeyPair and then save the obtained keypair.pem file in a safe location.
  For safety we can change the permissions of the keypair.pem file by using following command: 
        $chmod 400 <loaction_of_keyPair.pem>
3. Then start by creating a cluster in Amazon EMR and then give the keypair to that cluster and then start the cluster and then wait for it to generate a master public DNS Ip.

Once the DNS is obtained, we have to select Amazon EC2 and then select the master node and then have to set the SSH Rule with TCP protocol and our Local IP address and then set the port to 22.

Once the above process is done correctly, then we can connect to the cluster using the terminal in cloudera using the following commands.
    $ssh hadoop@<your_EC2_master_public_DNS_> -i <location_of_keypair.pem_in_hadoop>

Once the terminal is connected to EMR cluster, we can execute our programs easily using following commands.

Storing data from AWS to hadoop:

We need to store our source code and data files from AWS to hadoop by using following commands
       $ aws s3 cp s3://bucketname/<filename> ./
The command is repeated for all the files that are to be downloaded from AWS to hadoop.

Running Program: 

      $spark-submit s3://bucketname/<filename.py> s3n://bucketname/<hadoop_path_for_data_file> s3n://bucketname/<output_path>
Once the command is executed it takes a lot of time, to run the code, and then stores the output in Amazon S3.
We can either retrieve the files using the  browser or a desktop application called S3 Browser. 
 Multiple output files will be generated for our program. So once all the files are on your local machine, append all the files into one file. Thats the final recommendation. 

 1. user.py :

    The arguments are :  user.py, BX-Book-Ratings.csv, Output output_path

    Command: 
    $spark-submit s3://bucketname/<user.py> s3n://bucketname/<home/cloudera/BX-Book-Ratings.csv> s3n://bucketname/<home/cloudera/user_output>
  
  2. item.py :

  The arguments are :  item.py, BX-Book-Ratings.csv, Output output_path

  Command: 
  $spark-submit s3://bucketname/<item.py> s3n://bucketname/<home/cloudera/BX-Book-Ratings.csv> s3n://bucketname/<home/cloudera/item_output>


Check the item_output and user_output for the item based recomendations and user based recommendations. 
