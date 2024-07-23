# Func2ETL frontend

## Dev setup

- If you run against local it should work.
- If you run against prod you need to set up CORS support (https://chrome.google.com/webstore/detail/allow-cors-access-control/lhobafahddgcelffkeicbaginigeejlf). Click enable on the chrome extension
- Set up husky by running: `npm run configure-husky` in the `frontend/` directory

## Code generation

To generate the code for the typescript backend client, run : `npx @rtk-query/codegen-openapi openapi-config.json` from
the `frontend` directory, while the backend is running on localhost:8000.


## Building docker
## Dev mode
For development you'll want to run

```bash
./dev.sh --build # to build it all
./dev.sh # to pull docker images but use local code
```
## You need 9GB assigned to Docker or more to build the frontend
The frontend build requires around 8GB of memory to be assigned to docker to build.
If you run into this, bump your docker memory allocation up to 9GB or more.


## Prod mode
For production build you'll want to run

```bash
./prod.sh # to pull from docker and run
./prod.sh --build # to rebuild images for prod
```
### Caveats:
You'll want to clean the `backend/dist/` directory to not add unnecessary files to the docker image.


## Pushing
How to push to docker hub:
```bash
# retag if needed
docker tag local-image:tagname dagworks/ui-backend:VERSION
# push built image
docker push dagworks/ui-backend:VERSION
# retag as latest
docker tag dagworks/ui-backend:VERSION dagworks/ui-backend:latest
# push latest
docker push dagworks/ui-backend:latest
```

```bash
# retag if needed
docker tag local-image:tagname dagworks/ui-frontend:VERSION
# push built image
docker push dagworks/ui-frontend:VERSION
# retag as latest
docker tag dagworks/ui-backend:VERSION dagworks/ui-backend:latest
# push latest
docker push dagworks/ui-backend:latest
```
