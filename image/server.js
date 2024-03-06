// init colors
const chalk = require('chalk')
chalk.level = 1

// init metrics
const prom_client = require('prom-client');
const collectDefaultMetrics = prom_client.collectDefaultMetrics;
collectDefaultMetrics({ prefix: 'hnc_ui_' });

// init file reader
const fs = require('node:fs')

const express = require('express')
const app = express()
const server = require("http").createServer(app)

const k8s = require('./k8s')

const sa_k8s_api = new k8s(fs.readFileSync('/var/run/secrets/kubernetes.io/serviceaccount/token'))

let log_level = process.env.LOG_LEVEL

app.get('/logout', (req, res) => {
  res.redirect(301, process.env.LOGOUT_PATH)
})

app.get('/metrics', async (req, res) => {
  res.set('Content-Type', prom_client.register.contentType)
  res.end(await prom_client.register.metrics())
})

// verbose user info for every request
app.use(function (req,res,next) {
  if (process.env.USE_SA_TOKEN == "true") {
    req.user = "hnc service account";
    req.k8s_api = sa_k8s_api;
  } else {
    req.user = req.get('x-forwarded-user');
    req.k8s_api = new k8s(req.get('x-forwarded-access-token'))
  }
  next();
});

// serve all files in "public" folder
app.use(express.static(__dirname+"/public"));

// parse req body
app.use(express.urlencoded());
app.use(express.json());

// the client doesnt needs permissions to list objects cluster-wide scoped.
// so thats why the pod will listend for all namespaces and forwards their names to the user.
// (that way when new namespace/object is being created we know that the client cant miss it)

// the server is watching all the objects,
// i will use Server-Send Events (SSE) (when user asks some url, dont res.end(), instead just keep res.write()),
// the kubernetes watch lasts 5 minutes (etcd 3 history limit), so i will keep the SSE upto 5 mins as well (its greate for refreshing permissions)

// i think that each client will watch for his objects as well per namespace, because i wont wanna send api request each time something changes.
// so the server will listen for all namespaces, when namespace is added it will notify the clients, and the cliesnt will attempt to get the namespace info (ideally i would make the clie watch the namespace, but with view permissions on the namespace its not possible).
// the server will save the list of namespaces locally in cache (only the names), cuz i wont wanna do extra quesry each time client connects.

// API

function handle_error(err, res) {
  console.log(chalk.red(err))
  if (res) {
    if (err.statusCode) {
      res.status(err.statusCode)
    } else {
      res.status(500)
    }
    res.end(err.message)
  }
}

app.get('/api/get/username', (req, res) => {
  res.end(req.user)
})

async function user_watch_quota(req, res, ns_name) {
  const quota = {
    apiVersion: 'v1',
    kind: 'ResourceQuota',
    metadata: {
      namespace: ns_name
    }
  }

  await req.k8s_api.watch(quota, event => {
    res.write(`data: ${JSON.stringify(event)}\n\n`)
  })
  .then(async () => { // after first watching sucesfully ended
    // keep restart watches if timedout
    while (ns_clients[req.connection_id]) {
      await req.k8s_api.watch(quota, event => {
        res.write(`data: ${JSON.stringify(event)}\n\n`)
      })
    }
  })
  .catch(err => {
    res.write(`data: ${JSON.stringify({
      type: "ADDED",
      object: {
        kind: 'ResourceQuota',
        metadata: {
          namespace: ns_name
        },
        message: `couldnt watch hrq on namespace '${ns_name}', error: ${err.message}`
      }
    })}\n\n`)
  })
}

async function user_watch_hrq(req, res, ns_name) {
  const hrq = {
    apiVersion: 'hnc.x-k8s.io/v1alpha2',
    kind: 'HierarchicalResourceQuota',
    metadata: {
      namespace: ns_name
    }
  }

  await req.k8s_api.watch(hrq, event => {
    res.write(`data: ${JSON.stringify(event)}\n\n`)
  })
  .then(async () => { // after first watching sucesfully ended
    // keep restart watches if timedout
    while (ns_clients[req.connection_id]) {
      await req.k8s_api.watch(hrq, event => {
        res.write(`data: ${JSON.stringify(event)}\n\n`)
      })
    }
  })
  .catch(err => {
    res.write(`data: ${JSON.stringify({
      type: "ADDED",
      object: {
        kind: 'HierarchicalResourceQuota',
        metadata: {
          namespace: ns_name
        },
        message: `couldnt watch quota on namespace '${ns_name}', error: ${err.message}`
      }
    })}\n\n`)
  })
}

async function user_watch_network_policy(req, res, ns_name) {
  const network_policy = {
    apiVersion: 'networking.k8s.io/v1',
    kind: 'NetworkPolicy',
    metadata: {
      namespace: ns_name
    }
  }

  await req.k8s_api.watch(network_policy, event => {
    res.write(`data: ${JSON.stringify(event)}\n\n`)
  })
  .then(async () => { // after first watching sucesfully ended
    // keep restart watches if timedout
    while (ns_clients[req.connection_id]) {
      await req.k8s_api.watch(network_policy, event => {
        res.write(`data: ${JSON.stringify(event)}\n\n`)
      })
    }
  })
  .catch(err => {
    res.write(`data: ${JSON.stringify({
      type: "ADDED",
      object: {
        kind: 'NetworkPolicy',
        metadata: {
          namespace: ns_name
        },
        message: `couldnt watch network policy on namespace '${ns_name}', error: ${err.message}`
      }
    })}\n\n`)
  })
}

async function user_watch_hierarchy_config(req, res, ns_name) {
  const hierarchy_config = {
    apiVersion: 'hnc.x-k8s.io/v1alpha2',
    kind: 'HierarchyConfiguration',
    metadata: {
      namespace: ns_name
    }
  }

  await req.k8s_api.watch(hierarchy_config, event => {
    res.write(`data: ${JSON.stringify(event)}\n\n`)
  })
  .then(async () => { // after first watching sucesfully ended
    // keep restart watches if timedout
    while (ns_clients[req.connection_id]) {
      await req.k8s_api.watch(hierarchy_config, event => {
        res.write(`data: ${JSON.stringify(event)}\n\n`)
      })
    }
  })
  .catch(err => {
    res.write(`data: ${JSON.stringify({
      type: "ADDED",
      object: {
        kind: 'HierarchyConfiguration',
        metadata: {
          namespace: ns_name
        },
        message: `couldnt watch hierarchy config on namespace '${ns_name}', error: ${err.message}`
      }
    })}\n\n`)
  })
}

async function user_check_ns(req, res, ns_name) {
  const ns_template = {
    apiVersion: 'v1',
    kind: 'Namespace',
    metadata: {
      name: ns_name
    }
  }
  
  await req.k8s_api.get(ns_template)
  .then(ns => {
    res.write(`data: ${JSON.stringify({
      type: "MODIFIED",
      object: ns
    })}\n\n`)

    // watch objects inside the namespace
    user_watch_quota(req, res, ns_name)
    user_watch_hrq(req, res, ns_name)
    user_watch_network_policy(req, res, ns_name)
    user_watch_hierarchy_config(req, res, ns_name)
  })
  .catch(err => {
    res.write(`data: ${JSON.stringify({
      type: "DELETED",
      object: ns_template
    })}\n\n`)
  })
}

async function verify_token(req) {
  token_res = await sa_k8s_api.create({
    apiVersion: 'authentication.k8s.io/v1',
    kind: 'TokenReview',
    metadata: {
      name: 'test'
    },
    spec: {
      token: req.k8s_api.get_token()
    }
  })
  return token_res.status.authenticated == true
}

// stream namespaces + quota + hrq
app.get("/api/get/objects", async (req, res) => {
  // logout if token is invalid
  if (!verify_token(req)) {
    res.redirect(302, '/logout')
  }

  res.setHeader('Content-Type', 'text/event-stream')
  res.setHeader('Cache-Control', 'no-cache')
  res.setHeader('Connection', 'keep-alive')

  // keep connection alive to avoid chrome closing it
  const heartbeats = setInterval(() => {
    res.write('\n\n')
  }, 1000*20)

  // timeout after 5 min for permission updates + avoid bugs
  const close_con_timeout = setTimeout(close_con, 1000*60*5)
  function close_con() {
    delete ns_clients[req.connection_id]
    clearInterval(heartbeats)
    clearTimeout(close_con_timeout)
    res.end()
  }

  // subscribe the user for namespaces updates
  user_sub_watch(req, res)

  req.on('close', close_con)
})

function user_sub_watch(req, res) {
  // give this connection an id
  do {
    req.connection_id = Math.floor(Math.random() * 1000000000)
  } while (ns_clients[req.connection_id])
  // check for user any updated namespace
  ns_clients[req.connection_id] = ns_name => {
    if (log_level == "DEBUG") {
      console.log(`user ${req.user} got update for namespace ${ns_name}`)
    }
    user_check_ns(req, res, ns_name)
  }

  // go over ns cache and check namespaces
  Object.keys(ns_cache).forEach(ns_name => {
    user_check_ns(req, res, ns_name)
  })
}

// create namespace
app.post("/api/create/ns", async (req, res) => {
  const ns_name = req.get('ns-name')
  const parent_ns = req.get('parent-ns')

  await req.k8s_api.create({
    apiVersion: 'v1',
    kind: 'Namespace',
    metadata: {
      name: ns_name
    }
  })
  .then(async () => {
    if (parent_ns) {
      // create the namespace
      await req.k8s_api.create({
        apiVersion: 'hnc.x-k8s.io/v1alpha2',
        kind: 'HierarchyConfiguration',
        metadata: {
          name: 'hierarchy',
          namespace: ns_name
        },
        spec: {
          parent: parent_ns
        }
      })
      res.end(`successfully created namespace '${ns_name}'`)
    } else {
      res.end(`successfully created namespace '${ns_name}'`)
    }
  })
  .catch(err => handle_error(err, res))
})

// create sub-namespace
app.post("/api/create/sub-ns", async (req, res) => {
  const ns_name = req.get('ns-name')
  const parent_ns = req.get('parent-ns')

  if (parent_ns) {
    // create son CR, to automatically create namespace
    await req.k8s_api.create({
      apiVersion: 'hnc.x-k8s.io/v1alpha2',
      kind: 'SubnamespaceAnchor',
      metadata: {
        name: ns_name,
        namespace: parent_ns
      }
    }).then(() => {
      res.end(`successfully created namespace '${ns_name}'`)
    }).catch(err => handle_error(err, res))

  } else {
    res.status(400).end(`you must specify parent_ns header`)
  }
})

// delete namespace/sub-namespace
app.delete("/api/delete/ns", async (req, res) => {
  const ns_name = req.get('ns-name')

  await req.k8s_api.get({
    apiVersion: 'v1',
    kind: 'Namespace',
    metadata: {
      name: ns_name
    }
  })
  .then(ns => {
    const parent_ns = ns.metadata.annotations?.["hnc.x-k8s.io/subnamespace-of"]

    if (parent_ns) {
      // delete son CR, to automatically delete namespace
      req.k8s_api.delete({
        apiVersion: 'hnc.x-k8s.io/v1alpha2',
        kind: 'SubnamespaceAnchor',
        metadata: {
          name: ns_name,
          namespace: parent_ns
        }
      }).then(() => {
        res.end(`successfully deleted namespace '${ns_name}'`)
      }).catch(err => handle_error(err, res))
    } else {
      // delete the namespace
      req.k8s_api.delete({
        apiVersion: 'v1',
        kind: 'Namespace',
        metadata: {
          name: ns_name
        }
      }).then(() => {
        res.end(`successfully deleted namespace '${ns_name}'`)
      }).catch(err => handle_error(err, res))
    }
  })
  .catch(err => handle_error(err, res))
})

// apply yaml
app.put("/api/apply", async (req, res) => {
  await req.k8s_api.apply(req.body)
  .then(apply_res => {
    res.end(`succesfully applied object ${req.body.metadata.name}`)
  })
  .catch(err => handle_error(err, res))
})

// change parent ns
app.put("/api/change/parent", async (req, res) => {
  const ns_name = req.get('ns-name')
  const parent_ns = req.get('parent-ns')

  let hierarchy_conf = {
    apiVersion: 'hnc.x-k8s.io/v1alpha2',
    kind: 'HierarchyConfiguration',
    metadata: {
      name: "hierarchy",
      namespace: ns_name
    },
    spec: {}
  }

  // get the config CR
  req.k8s_api.get(hierarchy_conf).then(hierarchy_conf => {
    // modify the config CR for new parent
    if (parent_ns) {
      hierarchy_conf.spec.parent = parent_ns
    } else {
      delete hierarchy_conf.spec.parent
    }
    req.k8s_api.apply(hierarchy_conf).then(hierarchy_conf => {
      res.end(`changed parent ns to ${parent_ns}`)
    }).catch(err => handle_error(err, res))
  }).catch(err => {
    // if object doesnt exists then create it
    if (err.statusCode == 404) {
      if (parent_ns) {
        hierarchy_conf.spec.parent = parent_ns
      }
      req.k8s_api.create(hierarchy_conf)
    } else {
      this.verbose_error(err, url, object)
    }
  })
})

// change parent ns
app.put("/api/change/cascading", async (req, res) => {
  const ns_name = req.get('ns-name')
  const cascading = req.get('cascading') == "true"

  // get the config CR
  req.k8s_api.get({
    apiVersion: 'hnc.x-k8s.io/v1alpha2',
    kind: 'HierarchyConfiguration',
    metadata: {
      name: "hierarchy",
      namespace: ns_name
    }
  }).then(hierarchy_conf => {
    // modify the config CR for new parent
    hierarchy_conf.spec.allowCascadingDeletion = cascading
    req.k8s_api.apply(hierarchy_conf).then(hierarchy_conf => {
      res.end(`changed cascading deletion to ${cascading}`)
    }).catch(err => handle_error(err, res))
  }).catch(err => handle_error(err, res))
})

let ns_clients = {}
let ns_cache = {}
server_watch_ns()

const clients_metric = new prom_client.Gauge({ name: 'hnc_ui_clients', help: 'the number of clients that are currently watching "/api/get/objects" (namespaces + quotas + hrqs)' });
setInterval(() => {
  clients_metric.set(Object.keys(ns_clients).length)
}, 1000)

async function server_watch_ns() {
  const ns = {
    apiVersion: 'v1',
    kind: 'Namespace',
    metadata: {
      labels: {
        "hnc.x-k8s.io/included-namespace": "true"
      }
    }
  }

  // keep restart watches if timedout
  while (true) {
    // watch for any update like ADDED/MODIFIED/DELETED
    console.log(chalk.green(`starting watch on namespaces`))

    // reset cache if watch was restarted (good for avoiding bugs)
    if (log_level == "DEBUG") {
      console.log(`dumping namespaces cache:\n${ns_cache}`)
    }
    ns_cache = {}

    // keep updating the cache + clients
    await sa_k8s_api.watch(ns, event => {
      if (log_level == "DEBUG") {
        console.log(`namespace ${event.object.metadata.name} ${event.type}`)
      }
      update_ns_cache(event)
      Object.keys(ns_clients).forEach(client => {
        ns_clients[client](event.object.metadata.name)
      })
    })
  }
}

function update_ns_cache(event) {
  const obj = event.object
  switch(event.type) {
    case 'ADDED':
    case 'MODIFIED':
      ns_cache[obj.metadata.name] = true
      break
    case 'DELETED':
      delete ns_cache[obj.metadata.name]
      break
    default:
      console.log(chalk.red(`unhandled watch update type '${event.type}' when watching namespaces`))
  }
}

server.listen(8080, '127.0.0.1', function () {
  console.log(chalk.green('listening on port 8080'))
})
