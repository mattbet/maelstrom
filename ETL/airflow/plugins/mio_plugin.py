from airflow.plugins_manager import AirflowPlugin
from flask_appbuilder import BaseView, expose

# Definisci la tua vista personalizzata
class MiaNuovaVista(BaseView):
    route_base = "/mia_gui"

    @expose("/")
    def elenco(self):
        # Puoi anche fare il rendering di un file .html personalizzato
        return self.render_template("mio_template.html", contenuto="I miei dati dei DAG")

# Registra la vista all'interno della GUI di Airflow
class AirflowCustomGuiPlugin(AirflowPlugin):
    name = "mia_gui_estensione"
    views = [
        {
            "view": MiaNuovaVista(),
            "name": "Pannello DAG Personalizzato",
            "category": "Strumenti Custom"
        }
    ]
