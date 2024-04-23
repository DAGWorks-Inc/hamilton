


# Build caveats

## You need 9GB assigned to Docker or more to build the frontend
The frontend build requires around 8GB of memory to be assigned to docker to build.
If you run into this, bump your docker memory allocation up to 9GB or more.

How to push to docker hub:
```bash
docker tag local-image:tagname dagworks/ui-backend:LATEST
docker push dagworks/ui-backend:LATEST
```

```bash
docker tag local-image:tagname dagworks/ui-frontend:LATEST
docker push dagworks/ui-frontend:LATEST
```
