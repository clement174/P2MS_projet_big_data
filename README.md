# P2MS_projet_big_data
Projet fait dans le cadre de la formation Ecole IA microsoft (pipeline big data)

Les deux fichiers dans flask_API/cluster_part sont à envoyer sur le cluster utilisé pour le traitement spark.
Il faut changer l'adresse du cluster dans la fonction _process_on_cluster_and_save de l'objet Pipeline (flask_API/azure_customlib/process_pipeline.py)

La config se fait dans le fichier flask_API/app.py et dans le fichier config.json pour la partie sur le cluster.
