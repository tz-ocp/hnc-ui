const byline = require('byline')
const axios = require('axios')
const https = require('node:https')
const querystring = require('querystring')
const pluralize = require('pluralize')
const k8s_host = 'kubernetes.default.svc.cluster.local'

const default_opts = {
  headers: {
    "Accept": "application/json"
  },
  // for https/node-fetch: trust the kuberenetes api cert
  agent: new https.Agent({
    rejectUnauthorized: false
  }),
  // for axios: trust the kuberenetes api cert
  httpsAgent: new https.Agent({
    rejectUnauthorized: false
  })
}

class k8s {
  constructor(token) {
    this.set_token(token)
  }

  set_token(token) {
    this.opts = {
      ...default_opts,
      headers: {
        ...default_opts.headers,
        "Authorization": `Bearer ${token}`
      }
    }
  }

  get_token() {
    return this.opts.headers["Authorization"].replace("Bearer ","")
  }

  get(object) {
    const url = this.create_url(object).toString()
    return axios.get(url, this.opts).then(res => res.data.items ? res.data.items : res.data).catch(err => this.verbose_error(err, url, object))
  }

  create(object) {
    const url = this.create_url(object, false).toString()
    return axios.post(url, object, this.opts).then(res => res.data.items ? res.data.items : res.data).catch(err => this.verbose_error(err, url, object))
  }

  delete(object) {
    const url = this.create_url(object).toString()
    return axios.delete(url, this.opts).then(res => res.data.items ? res.data.items : res.data).catch(err => this.verbose_error(err, url, object))
  }

  apply(object) {
    const url = this.create_url(object).toString()
    return axios.put(url, object, this.opts).then(res => res.data.items ? res.data.items : res.data ).catch(err => {
      // if object doesnt exists then create it
      if (err.response.status == 404) {
        this.create(object)
      } else {
        this.verbose_error(err, url, object)
      }
    })
  }

  watch(object, use_event, run_after_connected) {
    let url = this.create_url(object, false)
    url.path += (url.path.includes('?') ? '&' : '?') + 'watch=true'
    if (object.metadata?.name) {
      url.path += '&' + querystring.stringify({fieldSelector: "metadata.name=" + object.metadata.name})
    }

    let watch_resolve
    let watch_reject
    const watch_promise = new Promise((resolve, reject) => {
      watch_resolve = resolve
      watch_reject = reject
    })

    const req = https.request({
      ...this.opts,
      ...url
    }, res => {
      const stream = byline(res)
      if (res.statusCode != 200) {
        doneCallOnce(new Error(`watching '${url.toString()}' failed, the return code isnt 200: '${res.statusCode}'`))
      } else {
        if (run_after_connected) {
          run_after_connected()
        }
        stream.on('data', line => {
          use_event(JSON.parse(line))
        })
        stream.on('error', (err) => {
          doneCallOnce(err)
        })
        stream.on('end', () => {
          doneCallOnce()
        })
        stream.on('close', () => {
          doneCallOnce()
        })
      }
    })

    let doneCalled = false;
    const doneCallOnce = (err) => {
        if (!doneCalled) {
            req.destroy();
            doneCalled = true;
            if (err) {
              watch_reject(err)
            } else {
              watch_resolve()
            }
        }
    }

    req.on('socket', socket => {
      socket.setTimeout(30000)
      socket.setKeepAlive(true, 30000)
    })
    req.on('error', err => {
      doneCallOnce(err)
    })
    req.on('end', () => {
      doneCallOnce()
    })
    req.on('close', () => {
      doneCallOnce()
    })

    req.end()

    return watch_promise
  }

  create_url(object, include_name=true) {
    if (!object.apiVersion) {
      throw new Error('The object passed must contain apiVersion field')
    }
    if (!object.kind) {
      throw new Error('The object passed must contain kind field')
    }

    let path = ""

    // add apiVersion
    if (object.apiVersion == "v1") {
      path += "/api/v1"
    } else {
      path += "/apis/" + object.apiVersion
    }

    // add namespace if object namespaced
    if (object.metadata?.namespace) {
      path += "/namespaces/" + object.metadata.namespace
    }

    // add object kind
    path += "/" + pluralize(object.kind.toLowerCase())

    // add object name
    if (include_name && object.metadata?.name) {
      path += "/" + object.metadata.name
    }

    // add label selector
    const labels = object.metadata?.labels
    if (labels) {
      path += "?" + querystring.stringify({labelSelector: Object.keys(labels).map(label => label + "=" + labels[label]).join(",")})
    }

    return {
      host: k8s_host,
      path: path,
      toString() {
        return "https://" + this.host + this.path
      }
    }
  }

  verbose_error(err, url, object) {
    let msg
    let statusCode
    if (err.response?.data?.message) {
      msg = err.response.data.message
      statusCode = err.response.data.code
    } else {
      msg = err.message
    }

    const new_err = new Error(`${msg}\nThe attempted url was: '${url}'\nThe attempted object was:\n${JSON.stringify(object)}`)
    new_err.statusCode = statusCode
    throw new_err
  }
}

module.exports = k8s
