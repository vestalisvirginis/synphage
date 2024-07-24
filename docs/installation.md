# Installation


## Via pip  <a id="pip-install"></a>

### Requirements
The following dependencies need to be installed in order to run synphage on your system.
 
- `Python 3.11`
- A Python package manager such as `Pip` or `uv`

- [Blast+](https://ftp.ncbi.nlm.nih.gov/blast/executables/blast+/) >= 2.12.0

Install [Python](https://www.python.org/) and [Blast+](https://ftp.ncbi.nlm.nih.gov/blast/executables/blast+/) using your package manager of choice, or by downloading an installer appropriate for your system from [python.org](https://www.python.org/downloads/) and from the [NCBI](https://ftp.ncbi.nlm.nih.gov/blast/executables/blast+/) respectively.  
The Python package manager `pip` is installed by default with Python, however you may need to upgrade pip to the latest version:
``` bash
pip install --upgrade pip
```


### Install `synphage`

`synphage`is available as a [Python package](https://pypi.org/project/synphage/) and can be install with the Python package manager `pip` in an opened terminal window.

=== "Linux/MacOS"
    ``` bash
    # Latest
    pip install synphage
    ```

=== "Windows"
    ``` bash
    # Latest
    python -m pip install synphage
    ```

=== "WSL"
    ``` bash
    # Add example for WSL + video
    ```

This will automatically install compatible versions of all Python dependencies.


### Run `synphage`  <a id="run-synphage-pip"></a>

1. Environment variables

    `synphage` uses the following environment variables:  
        - `INPUT_DIR` : for specifying the path to the folder containing the user's `GenBank files`. If not set, this path will be defaulted to the temp folder. This path can also be modified at run time.  
        - `OUTPUT_DIR`: for specifying the path to the folder where the data generated during the run will be stored. If not set, this path will be defaulted to the temp folder.  
        - `EMAIL` (optional): for connecting to the NCBI database.  
        - `API_KEY` (optional): for connecting to the NCBI database and download files.  
        - `DAGSTER_HOME` (optional): for storing metadata generated during former run of the pipeline

    ???+ info
        - `EMAIL` and `API_KEY` are only required for connecting to the NCBI database and downloading GenBank files. If the user only works with local data, these two variables can be ignored.
        - `DAGSTER_HOME` is only necessary to keep track of the previous run and generated metadata. Does not impair data storage if not set.

    ???+ tip
        These variables can be set with a `.env` file located in your working directory (Dagster will automatically load them from the .env file when initialising the pipeline) or can be passed in the terminal before starting to run synphage: 
        === ":material-file-document-outline: .env"
            ``` .env
            INPUT_DIR=path/to/my/data/
            OUTPUT_DIR=path/to/synphage/data
            EMAIL=user.email@email.com
            API_KEY=UserApiKey
            ```
        === ":octicons-terminal-16: bash"
            ``` bash
            export INPUT_DIR=<path_to_data_folder>
            export OUTPUT_DIR=<path_to_synphage_folder>
            export EMAIL=user.email@email.com
            export API_KEY=UserApiKey
            ```


2. Data Input and Output

    1. Data Input

        The input data are the GenBank files located in the `INPUT_DIR`. However paths to other data location can be passed at run time for loading data from another directory.

        ???+ warning
            - Only a single path can be configured per loading job run.
            - The use of special characters in file names, might causes errors downstream.

        ???+ note
            `.gb`and `.gbk` are both valid extension for genbank files


    2. Data Output

        All output data are located in the `OUTPUT_DIR` set by the user.  
        This directory can be reused in future runs if the user needs to process additional sequences or simply generate additional synteny diagrams. 
        

        ???+ warning
            - If no output directory is set, the data folder will be the temporary folder by default.
            Be aware that the naming convention for the temporary folder (temp/, tmp/, ...) depends on your system.


        ???+ tip
            The current data directory can be checked in the [config panel](jobs.md#dir-config) of the jobs.  


3. Start synphage via dagster web-based interface

    To start synphage run the following command:
    ``` bash
    dagster dev -h 0.0.0.0 -p 3000 -m synphage
    ```

    ???+ tip
        As synphage uses dagster-webserver, -h and -p flags are required to visualise the pipeline in your browser:  
        -h : Host to use for the Dagster webserver  
        -p : Port to use for the Dagster webserver

    To access the webserver, follow the link displayed in your terminal or copy/paste it in your web-browser. In this example:
    ``` bash
    http://0.0.0.0:3000
    ```

    <figure markdown="span">
    ![Start dagster](./images/installation_pip/start_dagster.png)
    <figcaption>Dagster running from the terminal and link to the webserver</figcaption>
    </figure>


1. Stop synphage

    After completing your work, you can close the web-browser and stop the process running in the terminal with ++ctrl+c++ .
    
    <figure markdown="span">
    ![Stop dagster](./images/installation_pip/stop_dagster.png)
    <figcaption>Dagster shutting down</figcaption>
    </figure>


## Via synphage docker image  <a id="docker-install"></a>

### Requirements
The following dependency needs to be installed in order to run synphage Docker Image on your system.

- `Docker` or `Docker Desktop`


=== "Linux"
    - Install [docker desktop](https://www.docker.com/products/docker-desktop/) from the executable.  
    - Check the full documentation for [docker Linux](https://docs.docker.com/desktop/install/linux-install/).  
  
=== "MacOS"
    - Install [docker desktop](https://www.docker.com/products/docker-desktop/) from the executable.  
    - Check the full documentation for [docker Mac](https://docs.docker.com/desktop/install/mac-install/).  

=== "Windows"
    - Install [docker desktop](https://www.docker.com/products/docker-desktop/) from the executable.  
    - Check the full documentation for [docker Windows](https://docs.docker.com/desktop/install/windows-install/).


???+ info
    When installing docker from the website, the right version should automatically be selected for your computer.
    ![Docker-Desktop download](./images/dd_download.png){ align=right }


### Pull synphage image

=== ":simple-docker: Docker Desktop"
    1. Open the docker desktop app and go to `Images`.  
        ![Images](./images/dd_images.png){align=right}  

    2. Go to the search bar and search for `synphage`.  
        ![Search synphage image in DockerHub](./images/dd_pull_image.png){align=right}
        
        ???+ note
            The latest image will automatically be selected - advised

    3. Pull the image.  
        Select `Pull` and wait for the download to complete.

    4. synphage docker image is installed
        ![Installed image](./images/dd_pulled_image.png){align=right} 

    ???+ note
        Your Dashboard might look a bit different depending on the Docker Desktop version and your OS.
 
=== ":octicons-terminal-16: Bash"
    ```bash
    # Pull the image from docker hub
    docker pull vestalisvirginis/synphage:<tag>

    # Check the list of installed Docker Images
    docker image ls
    ```
    Replace `<tag>` with the [latest image tag](https://hub.docker.com/r/vestalisvirginis/synphage/tags).


### Run `synphage` container  <a id="run-synphage-container"></a>

#### Start the container  

=== ":simple-docker: Docker Desktop"
    1. Start the container
        ![Start container](./images/dd_start_container.png){align=right}  

    2. Open the drop-down menu `Optional settings`:  
        ![Optional settings pop-up window](./images/dd_optional_settings_1.png){width=500}  

    3. Set the `host port` to 3000  
   
        ???+ tip  
            Setting the port is required to run synphage as it uses a web-interface.  
            3000 is given as example, any other `available` port can be used.
  

          ![Port](./images/dd_optional_settings_2.png){width=500}

        ???+ warning
            Make sure that the port is available and not already in use (by another running container for example).

    4. Set the `Volumes`

        1. Data Output  
            All output data are located in the `/data` directory of the container.  
            The output data can be copied after the run from the /data folder or they can be stored in a `Docker Volume` that can be mounted to a new Docker Container and reused in subsequent run if the user needs to process additional sequences or simply generate additional synteny diagrams.

            === "Docker Volume"
                <figure markdown="span">
                ![Create volumes](./images/dd_volumes.png) 
                <figcaption>Create a Docker Volume for your data</figcaption>
                ![Volume Output Data](./images/dd_optional_settings_2.png){width=500}
                <figcaption>Mount your volume to the docker data volume when starting your container</figcaption>
                </figure>
            === "download"
                <figure markdown="span">
                ![Save data generated in the container](./images/dd_save_data_folder.png){align=right}
                <figcaption>Download the data from the container to you computer</figcaption>
                </figure>


        2. Dagster home  
            Metadata generated during the successive runs of the pipeline are stored in `/dagster` directory.  
            Setting a `DAGSTER_HOME` Volume is only necessary to keep track of the previous run and generated metadata. It does not impair data storage if not set.  

        ???+ danger
            All the data will be deleted when the container will be removed.
            If no Volume is mounted to the /data directory and the user do not save the data, data will be lost.


    5. Set the environment variables (optional)  
        `synphage` uses the following environment variables:  
        - `EMAIL` (optional): for connecting to the NCBI database.  
        - `API_KEY` (optional): for connecting to the NCBI database and download files.  
        - `DAGSTER_HOME` (optional): for storing metadata generated during former run of the pipeline
         
        ![Environment variables](./images/dd_env_variable.png){width=500}

        ???+ info
            - `EMAIL` and `API_KEY` are only required for connecting to the NCBI database and downloading GenBank files. If the user only works with local data, these two variables can be ignored.


    6. Press the `Run` button  
        Your container is now running.


    7. Import local GenBank files (optional)  
        `/user_files` is the directory that received users GenBank files.  
        For using locally stored GenBank files, the files can be imported or dragged and dropped (depending on your system) into the `/user_files` directory.
   
        ![Drag and drop genbank files](./images/installation_docker/dd_import_gb_files.png)

        ???+ warning
            - The use of special characters in file names, might causes errors downstream.

        ???+ note
            `.gb`and `.gbk` are both valid extension for genbank files


    9.  Connect to the web interface  
        To connect to the web-interface, select the link to the port or copy this link to your web-browser.
        ![Open the link to the web-interface](./images/dd_web_interface.png) 

    
    1. Stop and remove your container  
        After completing your work, you can close the web-browser and stop the container. After stopping your container a good practice is to remove it.  

        <figure markdown="span">
        ![Stop container](./images/dd_stop_container.png)
        <figcaption>Stop the container</figcaption>
        ![Remove container](./images/dd_rm_container.png) 
        <figcaption>Remove the container</figcaption>
        </figure>
 

=== ":octicons-terminal-16:  Bash"

    1. Environment variables

        `synphage` uses the following environment variables:  
            - `EMAIL` (optional): for connecting to the NCBI database.  
            - `API_KEY` (optional): for connecting to the NCBI database and download files.  

        ???+ info
            `EMAIL` and `API_KEY` are only required for connecting to the NCBI database and downloading GenBank files. If the user only works with local data, these two variables can be ignored.
        
        ???+ tip
            These variables can be passed in the terminal before starting to run synphage: 

            ``` bash
            export EMAIL=user.email@email.com
            export API_KEY=UserApiKey
            ```

    2. Start the container

        To run the container run the following command line:
        ```bash
        docker run -d --rm --name my_phage_box -p 3000:3000 vestalisvirginis/synphage:<tag>
        ```

        ???+ note
            The <tag> corresponds to the <tag> of the downloaded image.

        ???+ tip
            - As synphage uses dagster-webserver, -p flag is required to visualise the pipeline in your browser:   
            -p : [host_port:container_port]  
            The container_port is fixed to 3000.

            - To access the webserver, follow the link displayed in your browser or copy/paste it in your web-browser. In this example:
            ``` bash
            http://0.0.0.0:3000
            ```

        ???+ tip
            - It is good practice to name your container to find them easily: `--name`
            - It is also good practice to remove the container at the end of the run. By passing the `--rm` flag, the container will be automatically removed after being stopped.  


    3. Set the `Volumes`

        1. Data Output  
            All output data are located in the `/data` directory of the container.  
            The output data can be copied after the run from the /data folder or they can be stored in a `Docker Volume` that can be mounted to a new Docker Container and reused in subsequent run if the user needs to process additional sequences or simply generate additional synteny diagrams.

            === "Docker Volume"
                ```bash
                # Create volume synphage_data
                docker volume create synphage_data

                # Mount the volume to the /data directory in the container
                docker run -d --rm --name my_phage_box -v synphage_data:/data -p 3000:3000 vestalisvirginis/synphage:<tag>
                ```
            
            === "copy"
                ```bash
                docker cp container-id/data/* your/local/data_directory/
                ```

        2. Dagster home  
            Metadata generated during the successive runs of the pipeline are stored in `/dagster` directory.  
            Setting a `DAGSTER_HOME` Volume is only necessary to keep track of the previous run and generated metadata. It does not impair data storage if not set.
            ```bash
            # Create volume synphage_data
            docker volume create synphage_data
            docker volume create dagster_home

            # Mount the volume to the /data directory in the container
            docker run -d --rm --name my_phage_box -v synphage_data:/data -v dagster_home:/dagster -p 3000:3000 vestalisvirginis/synphage:<tag>
            ```


        ???+ danger
            All the data will be deleted when the container will be removed.
            If no Volume is mounted to the /data directory and the user do not save the data, data will be lost.

        ???+ warning
            Volume names must be unique. You canot set two volumes wit the same name.


    1. Import local GenBank files (optional)  
        `/user_files` is the directory that received users GenBank files.  
        For using locally stored GenBank files, the files can be copied into the `/user_files` directory.  
        ```bash
        docker cp path_to_my_gb_files/*.gb* container_id:/user_files
        ```

        ???+ tip
            Start first the container and then copy the files into the container.

        ???+ warning
            - The use of special characters in file names, might causes errors downstream.

        ???+ note
            `.gb`and `.gbk` are both valid extension for genbank files


    2.  Connect to the web interface  
        To connect to the web-interface, select the link to the port or copy this link to your web-browser.
        ```bash
        http://0.0.0.0:3000
        ```

         <figure markdown="span">
        ![Start dagster](./images/installation_docker/start_docker_container.png)
        <figcaption>Dagster running at the start of the docker container</figcaption>
        </figure>


    3. Stop synphage

        After completing your work, you can close the web-browser and stop the process running in the terminal with ++ctrl+c++ .

        <figure markdown="span">
        ![Stop dagster](./images/installation_docker/stop_docker_container.png)
        <figcaption>Dagster shutting down and the docker container is stopped and removed automatically</figcaption>
        </figure>
        s