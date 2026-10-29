# Maelstrom

>This project aim to setup a workflow to collect data from various source as:
>- Virtualization platforms
>- Infrastructure monitoring software
>- SIEM and threat intelligence platforms
>- Directory services
>
> All this data will be collected, reconciled, and entered into a single CMDB to support governance and operations functions.
> All sotwares used will be Open Source or bespoke. 
 
# Development tools


|  |  | 
| --- | --- | 
|  [Notepad++](https://notepad-plus-plus.org/)  | Notepad++ is a free (as in “free speech” and also as in “free beer”) source code editor and Notepad replacement that supports several programming languages.  | 
| [MD WYSIWYG Editor](https://chromewebstore.google.com/detail/md-wysiwyg-editor/elmdnkbmhoofobbpboiakienehhahcde)   | A powerful Markdown editor with split-view layout, CodeMirror integration, bidirectional sync,  and LaTeX math support. | 
| [Docker Desktop](https://www.docker.com/products/docker-desktop/)   |  The #1 containerization software for developers and teams | 
|  [LibreDB Studio](https://github.com/libredb/libredb-studio) |  The database editor that deploys next to your data, not onto your laptop. | 
| [VS code](https://code.visualstudio.com/)  | The open source AI code editor  | 


# Applications 

## [ETL](https://github.com/mattbet/maelstrom/tree/main/ETL)

## DATABASE
[PostgreSQL](https://www.postgresql.org/)

The World's Most Advanced Open Source Relational Database

# Development toolchain 

![Image](https://github.com/mattbet/maelstrom/blob/main/Docs/Flusso_Sviluppo.png)

VS Code is used with plugins for Python, containers, and GitHub. Code is written, tested, and updated within the repository. Certain code review stages are carried out using Notepad++ on the same files.
Firefox is used to interact with AirFlow and the database via LibreDB Studio; although local clients are available, I opted to build a complete, self-contained solution that is easy to redistribute.
For editing Markdown files, I have used a Chrome plugin, for the sake of simplicity.
