FROM airhacks/glassfish
COPY ./target/tropes.war ${DEPLOYMENT_DIR}
