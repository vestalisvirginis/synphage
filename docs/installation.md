# Coming Soon

We’re working hard to make this page available by the end of June! 

Until then we look forward to your questions and feedbacks using the [Discussions](https://github.com/vestalisvirginis/synphage/discussions) or the [Issues](https://github.com/vestalisvirginis/synphage/issues) pages.  
Please follow the [How to contribute?](https://github.com/vestalisvirginis/synphage/blob/main/CONTRIBUTING.md) guidelines for any contribution to this project!

We are looking forward to hearing from you!




# Installation

## Pre-requisite

`synphage` relies on one non-python dependency, [Blast+](https://ftp.ncbi.nlm.nih.gov/blast/executables/blast+/) >= 2.12.0, that need to be manually installed when synphage is installed with `pip`.

<a id="pip-install"></a>
## Via pip 

`synphage`is available as a [Python package](https://pypi.org/project/synphage/) and can be install with the Python package manager `pip` in an opened terminal window.

=== "Linux/MacOS"
    ```bash
    # Latest
    pip install synphage
    ```

=== "Windows"
    ```bash
    # Latest
    python -m pip install synphage
    ```

This will automatically install compatible versions of all Python dependencies.

<a id="docker-install"></a>
## Via docker 

### Pre-requisite : docker

In order to use `synphage` Docker Image, you first need to have docker installed.

=== "Linux"
    - Install [docker desktop](https://www.docker.com/products/docker-desktop/) from the executable.  
    - Check the full documentation for [docker Linux](https://docs.docker.com/desktop/install/linux-install/).  
  
=== "MacOS"
    - Install [docker desktop](https://www.docker.com/products/docker-desktop/) from the executable.  
    - Check the full documentation for [docker Mac](https://docs.docker.com/desktop/install/mac-install/).  

=== "Windows"
    - Install [docker desktop](https://www.docker.com/products/docker-desktop/) from the executable.  
    - Check the full documentation for [docker Windows](https://docs.docker.com/desktop/install/windows-install/).

![Docker-Desktop download](./images/dd_download.png){ align=right }

???+ info
    When installing docker from the website, the right version should automatically be selected for your computer.


### Pull synphage image

=== "Docker Desktop"
    1. Open the docker desktop app and go to `Images`.  
    ![Images](./images/dd_images.png){align=right}  

    2. Go to the search bar and search for `synphage`.  
    ![Search synphage image in DockerHub](./images/dd_pull_image.png){align=right}  

    3. Pull the image (it will automatically select for the latest image (advised)).   

    4. The synphage image is installed
    ![Installed image](./images/dd_pulled_image.png){align=right} 

    ???+ note
        Your Dashboard might look a bit different depending on the Docker Desktop version and your OS.
 
=== "Bash"
    ```bash
    # Pull the image from docker hub
    docker pull vestalisvirginis/synphage:latest

    # List installed Docker Images
    docker image ls
    ```
    It will download the latest image. If a former version is desired, reeplace latest by version tag (e.g. `0.0.6`).

### Run `synphage` container

=== "Docker Desktop"
    1. Start the container
    ![Start container](./images/dd_start_container.png){align=right}  

    2. Open the drop-down menu `Optional settings`:  
    ![Optional settings pop-up window](./images/dd_optional_settings_1.png){align=right}  

    3. Set the `port` to 3000 (or any other port still available on your computer).  
    Port is the only setting required for running the program, as it uses a web-interface.   
    ![Optional settings example](./images/dd_optional_settings_2.png){align=right}  

    4. Set `EMAIL` and `API_KEY` environment variables (optional). These variables are only required if you want to use the `NCBI_download` job.
    ![Environment variables](./images/dd_env_variable.png){align=right} 

    5. Press the `Run` botton.

    6. In `Containers -> Files` : Drag and drop your genbank files in the `/data/genbank` directory of your running container
    ![Drag and drop genbank files](./images/dd_drag_and_drop_gb_files.png){align=right}

    ???+ warning
        The use of spaces and special characters in file names, might cause error downstream.

    ???+ note
        `.gb`and `.gbk` are both valid extension for genbank files

    7. For ploting add a `sequences.csv` file in the /data directory. Please use the file editor of the docker to check that the format of your file is according to the example below:
    ```txt
    168_SPbeta.gb,0
    Phi3T.gb,1
    ```

    Example of incorrectly formatted csv file (can happen when saved from excel):
    ![Incorrectly formatted csv file](./images/dd_csv_file_excel.png){align=right}

    Example of correctly formatted csv file:  
    ![Correctly formatted csv file](./images/dd_csv_file_correctly_formatted.png){align=right}

    ???+ warning
        Please here use **only** `.gb` as file extension.

    ???+ info
        The integer after the comma represents the orientation of the sequence in the synteny diagram.
        0 : sequence
        1 : reverse

    8. Connect to the web interface
    ![Open the link to the web-interface](./images/dd_web_interface.png) 
 
=== "Bash"

    1. Environnment variable 
    Only required if you want to use the `NCBI_download` job.
    The variables can be exported before starting the container or a .env file can be copied into the working directory.

        === "export"
        ```bash
        export EMAIL=john.doe@domain.com
        export API_KEY=gdzjdfzkhlh6832HBkh
        ```

        === ".env file"
        ```text
        EMAIL=john.doe@domain.com
        API_KEY=gdzjdfzkhlh6832HBkh
        ```

        ???+ note
            Dagster will recognise and import environment variable from the .env file automatically.

    
    2. Start the container
    ```bash
    docker run -d --rm --name my_phage_box -p 3000:3000 vestalisvirginis/synphage:latest
    ```

    3. Copy genbank files in the `/data/genbank/` directory of the container
    ```bash
    docker cp path_to_my_gb_files/*.gb container_id:/data/genbank/
    ```

        ???+ warning
            The use of spaces and special characters in file names, might cause error downstream.

        ???+ note
            `.gb`and `.gbk` are both valid extension for genbank files

    4. For ploting add a sequences.csv file in the /data directory. Format your file according to the example below:
    ```txt
    168_SPbeta.gb,0
    Phi3T.gb,1
    ```
    ```bash
    # Create file
    touch sequences.csv
    # Edit file
    vim sequences.csv
    # Copy file to the /data directory
    docker cp path_to_file/sequences.csv /data/
    ```

        ???+ warning
            Please here use **only** `.gb` as file extension.

        ???+ info
            The integer after the comma represents the orientation of the sequence in the synteny diagram.
            0 : sequence
            1 : reverse

    5. Open localhost:3000 in your web-browser.