# Usage
helm repo add hnc-ui https://zfrhv.github.io/hnc-ui  
helm install test hnc-ui/hnc-ui

# Concept
### Basic workflow
1. The hnc-ui watches for namespaces that is included by hnc (via label `hnc.x-k8s.io/included-namespace: "true"`) with its service account token.
2. each client that connects attempts to watch each namespaces that hnc-ui sees + hrqs in same namespace + quota using the user token. If env `USE_SA_TOKEN: "true"` specified then serviceAccount token is used instead.

Example values.yaml are located inside the chart under [`examples`](./hnc-ui/examles) directory.

## Client side (index.html)
The tree diagram is made with `d3.js`.  

# Example pictures:
![hnc example 1 image](docs/hnc.png)
![hnc example 2 image](docs/hnc2.png)
