# Maelstrom

> Configurare un insieme di strumenti opensource che raccolga dati da varie sorgenti e ne permetta la riconciliazione, per alimentare un CMDB.
 
# Strumenti di sviluppo utilizzati


|  |  | 
| --- | --- | 
|  [Notepad++](https://notepad-plus-plus.org/)  | Notepad++ is a free (as in “free speech” and also as in “free beer”) source code editor and Notepad replacement that supports several programming languages.  | 
| [MD WYSIWYG Editor](https://chromewebstore.google.com/detail/md-wysiwyg-editor/elmdnkbmhoofobbpboiakienehhahcde)   | A powerful Markdown editor with split-view layout, CodeMirror integration, bidirectional sync,  and LaTeX math support. | 
| [Docker Desktop](https://www.docker.com/products/docker-desktop/)   |  The #1 containerization software for developers and teams | 
|  [LibreDB Studio](https://github.com/libredb/libredb-studio) |  The database editor that deploys next to your data, not onto your laptop. | 
| [VS code](https://code.visualstudio.com/)  | The open source AI code editor  | 


# Applicativi  utilizzati

## [ETL](https://github.com/mattbet/cmdb/tree/main/ETL)

## DATABASE
[PostgreSQL](https://www.postgresql.org/)

The World's Most Advanced Open Source Relational Database

# Catena di sviluppo

![Image](https://github.com/mattbet/maelstrom/blob/main/Docs/Flusso_Sviluppo.png)

VS Code viene utilizzato con i plugin per Python, container e Github. Il codice viene scritto, testato ed aggiornato sul repository. Alcune fasi di revisione del codice vengono svolte con Notepad++ sugli stessi file.
Firefox viene usato per intergire con AirFlow e con il DB tramite LibreDB Studio, pur avendo client locali ho preferito costruire una soluzione completa e autonoma nell'uso, da redistribuire.
Per ditare i fiel markdown ho utilizzato un pulin per Chorme, per semplicità.
