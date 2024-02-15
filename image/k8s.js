const axios = require('axios')
const https = require('node:https')
const querystring = require('querystring')
const pluralize = require('pluralize')
const k8s_host = 'kubernetes.default.svc'

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
    this.update_token(token)
  }

  update_token(token) {
    this.opts = {
      ...default_opts,
      headers: {
        ...default_opts.headers,
        "Authorization": `Bearer ${token}`
      }
    }
  }

  get(object) {
    const url = this.create_url(object).toString()
    return axios.get(url, this.opts).then((res) => res.data.items ? res.data.items : res.data).catch(err => this.verbose_error(err, url, object))
  }

  create(object) {
    const url = this.create_url(object, false).toString()
    return axios.post(url, object, this.opts).catch(err => this.verbose_error(err, url, object))
  }

  delete(object) {
    const url = this.create_url(object).toString()
    return axios.delete(url, this.opts).catch(err => this.verbose_error(err, url, object))
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
      if (res.statusCode != 200) {
        if (500 > res.statusCode && res.statusCode >= 400) {
          watch_reject(new Error(`watching '${url.toString()}' failed, the return code was '${res.statusCode}'`))
        } else {
          watch_reject(new Error(`watching '${url.toString()}' failed, the return code isnt 200: '${res.statusCode}'`))
        }
      } else {
        if (run_after_connected) {
          run_after_connected()
        }
        res.setEncoding('utf8')
        let buffer = ""
        res.on('data', chunk => {
          buffer += chunk
          const events = buffer.split('\n')
          events.forEach((event, index) => {
            let event_obj
            try {
              // try parsing event to object
              event_obj = JSON.parse(event)
            } catch(err) {
              // if not last event then something broken inside the objects
              if (index != events.length-1) {
                throw `error parsing buffer while watching objects from k8s, the buffer is:\n${buffer}\nthe error is: \n${err}`
              }
            }
  
            // forward the event if succesfully parsed
            if (event_obj) {
              use_event(event_obj)
            }
  
            // after last even reset the buffer
            if (index == events.length - 1) {
              if (event_obj) {
                buffer = ""
              } else {
                buffer = event
              }
            }
          })
        })
        res.on('end', () => {
          watch_resolve()
        })
      }
    })

    req.on('error', err => {
      watch_reject(err)
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
