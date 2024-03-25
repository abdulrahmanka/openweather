# openweather
How to run the pipeline 
1. Clone the repo https://github.com/abdulrahmanka/openweather-etl.git and place it in the same level as the openweather folder.
2. Run  `docker-compose up` to start the postgres.
3. Run the sql/init.sql in postgres if running the first time.
4. Run the script run-mage-dev.bat
5. Add OpenWeather API key in mage UI.
To add secrets in Mage, you should go to the edit page for a pipeline. Within the sidekick, you'll find a Secrets tab. Here, you can create a new secret by pressing the New button. You will need to input a name for the secret and its value, then press Enter or Return to save it. These secrets can be used across the project they are created in. Currently, secrets can be managed at the project level, with pipeline level secrets to be supported soon.
