import docker

# Define the path to the file on your local machine
local_file_path = "C:/Users/ADMIN/Desktop/Data_Crypto/Get_recent_data.py"

# Define the path inside the Docker container where you want to copy the file
container_file_path = "/opt/airflow/dags/Get_recent_data.py"

# Initialize the Docker client
client = docker.from_env()

# Find the ID or name of the Docker container
container_name_or_id = "material-airflow-webserver-1"  # Replace with your container's ID or name

# Copy the file to the container
try:
    with open(local_file_path, "rb") as local_file:
        container = client.containers.get(container_name_or_id)
        container.put_archive("/", data=local_file.read(), path=container_file_path)
    print(f"File copied to the container: {container_file_path}")
except docker.errors.NotFound:
    print(f"Container {container_name_or_id} not found.")
except Exception as e:
    print(f"An error occurred: {e}")