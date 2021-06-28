/**
 * Created by zhonghuiping on 2017/8/25.
 */
import request  from '../utils/request';

window.datacollectorUrl = ''

/* window.datacollectorUrl = 'http://localhost:12581' */

export async function getSourceOptions(params) {
  return request(window.datacollectorUrl + '/datacollector/reader/usages', {
    method: 'get',
  });
}

export async function getSourceOptionsFormData(params) {
  return request(window.datacollectorUrl + '/datacollector/reader/options', {
    method: 'get',
  });
}

export async function getSourceParseOptionsFormData(params) {
  return request(window.datacollectorUrl + '/datacollector/parser/options', {
    method: 'get',
  });
}

export async function getSourceParseOptions(params) {
  return request(window.datacollectorUrl + '/datacollector/parser/usages', {
    method: 'get',
  });
}

export async function getSourceParsesamplelogs(params) {
  return request(window.datacollectorUrl + '/datacollector/parser/samplelogs', {
    method: 'get',
  });
}

export async function getSenderOptionsFormData(params) {
  return request(window.datacollectorUrl + '/datacollector/sender/options', {
    method: 'get',
  });
}

export async function getSenderOptions(params) {
  return request(window.datacollectorUrl + '/datacollector/sender/usages', {
    method: 'get',
  });
}

export async function getMetricKeys(params) {
  return request(window.datacollectorUrl + '/datacollector/metric/keys', {
    method: 'get',
  });
}

export async function getMetricUsages(params) {
  return request(window.datacollectorUrl + '/datacollector/metric/usages', {
    method: 'get',
  });
}

export async function getMetricOptions(params) {
  return request(window.datacollectorUrl + '/datacollector/metric/options', {
    method: 'get',
  });
}

export async function getRunnerConfigs(params) {
  return request(window.datacollectorUrl + '/datacollector/configs', {
    method: 'get',
  });
}

export async function getTransformOptions(params) {
  return request(window.datacollectorUrl + '/datacollector/transformer/options', {
    method: 'get',
  });
}

export async function getTransformConfigs(params) {
  return request(window.datacollectorUrl + '/datacollector/transformer/sampleconfigs', {
    method: 'get',
  });
}

export async function getTransformUsages(params) {
  return request(window.datacollectorUrl + '/datacollector/transformer/usages', {
    method: 'get',
  });
}


export async function getRunnerStatus(params) {
  return request(window.datacollectorUrl + '/datacollector/status', {
    method: 'get',
  });
}

export async function postParseData(params) {
  return request(window.datacollectorUrl + '/datacollector/parser/parse', {
    method: 'post',
    headers: {
      'Content-Type': 'application/json'
    },
    body: JSON.stringify(params.body),
  });
}

export async function postConfigData(params) {
  return request(window.datacollectorUrl + '/datacollector/configs/' + params.name, {
    method: 'post',
    headers: {
      'Content-Type': 'application/json'
    },
    body: JSON.stringify(params.body),
  });
}

export async function putConfigData(params) {
  return request(window.datacollectorUrl + '/datacollector/configs/' + params.name, {
    method: 'put',
    headers: {
      'Content-Type': 'application/json'
    },
    body: JSON.stringify(params.body),
  });
}

export async function deleteConfigData(params) {
  return request(window.datacollectorUrl + '/datacollector/configs/' + params.name, {
    method: 'delete'
  });
}

export async function getRunnerVersion(params) {
  return request(window.datacollectorUrl + '/datacollector/version', {
    method: 'get',
  });
}

export async function startRunner(params) {
  return request(window.datacollectorUrl + '/datacollector/configs/' + params.name + '/start', {
    method: 'post',
    headers: {
      'Content-Type': 'application/json'
    }
  });
}

export async function stopRunner(params) {
  return request(window.datacollectorUrl + '/datacollector/configs/' + params.name + '/stop', {
    method: 'post',
    headers: {
      'Content-Type': 'application/json'
    }
  });
}

export async function resetConfigData(params) {
  return request(window.datacollectorUrl + '/datacollector/configs/' + params.name + '/reset', {
    method: 'post',
    headers: {
      'Content-Type': 'application/json'
    }
  });
}

/* cluster api

 */
export async function postClusterConfigData(params) {
  return request(window.datacollectorUrl + '/datacollector/cluster/configs/' + params.name + '?tag=' + params.tag + '&url=' + params.url, {
    method: 'post',
    headers: {
      'Content-Type': 'application/json'
    },
    body: JSON.stringify(params.body),
  });
}

export async function putClusterConfigData(params) {
  return request(window.datacollectorUrl + '/datacollector/cluster/configs/' + params.name + '?tag=' + params.tag + '&url=' + params.url, {
    method: 'put',
    headers: {
      'Content-Type': 'application/json'
    },
    body: JSON.stringify(params.body),
  });
}

export async function getClusterRunnerConfigs(params) {
  return request(window.datacollectorUrl + '/datacollector/cluster/configs?tag=' + params.tag + '&url=' + params.machineUrl, {
    method: 'get',
  });
}

export async function getClusterRunnerStatus(params) {
  return request(window.datacollectorUrl + '/datacollector/cluster/status?tag=' + params.tag + '&url=' + params.machineUrl, {
    method: 'get',
  });
}

export async function getClusterSlaves(params) {
  return request(window.datacollectorUrl + '/datacollector/cluster/slaves', {
    method: 'get',
  });
}

export async function postClusterSlaveTag(params) {
  return request(window.datacollectorUrl + '/datacollector/cluster/slaves/tag?tag=' + params.name + '&url=' + params.url, {
    method: 'post',
    headers: {
      'Content-Type': 'application/json'
    },
    body: JSON.stringify(params.body),
  });
}

export async function deleteClusterSlaveTag(params) {
  return request(window.datacollectorUrl + '/datacollector/cluster/slaves?tag=' + params.name + '&url=' + params.url, {
    method: 'delete'
  });
}

export async function postClusterStopSlaveTag(params) {
  return request(window.datacollectorUrl + '/datacollector/cluster/configs/' + params.name + '/stop?tag=' + params.tag + '&url=' + params.url, {
    method: 'post'
  });
}

export async function postClusterResetSlaveTag(params) {
  return request(window.datacollectorUrl + '/datacollector/cluster/configs/' + params.name + '/reset?tag=' + params.tag + '&url=' + params.url, {
    method: 'post'
  });
}


export async function startClusterRunner(params) {
  return request(window.datacollectorUrl + '/datacollector/cluster/configs/' + params.name + '/start?tag=' + params.tag + '&url=' + params.url, {
    method: 'post',
    headers: {
      'Content-Type': 'application/json'
    }
  });
}

export async function postClusterDeleteSlaveTag(params) {
  return request(window.datacollectorUrl + '/datacollector/cluster/configs/' + params.name + '?tag=' + params.tag + '&url=' + params.url, {
    method: 'delete'
  });
}

export async function stopClusterRunner(params) {
  return request(window.datacollectorUrl + '/datacollector/cluster/configs/' + params.name + '/stop?tag=' + params.tag + '&url=' + params.url, {
    method: 'post',
    headers: {
      'Content-Type': 'application/json'
    }
  });
}

export async function resetClusterConfigData(params) {
  return request(window.datacollectorUrl + '/datacollector/cluster/configs/' + params.name + '/reset?tag=' + params.tag + '&url=' + params.url, {
    method: 'post',
    headers: {
      'Content-Type': 'application/json'
    }
  });
}

export async function deleteClusterConfigData(params) {
  return request(window.datacollectorUrl + '/datacollector/cluster/configs/' + params.name + '?tag=' + params.tag + '&url=' + params.url, {
    method: 'delete'
  });
}

export async function getClusterConfigData(params) {
  return request(window.datacollectorUrl + '/datacollector/cluster/configs/' + params.name + '?tag=' + params.tag + '&url=' + params.url, {
    method: 'get'
  });
}

export async function getIsCluster(params) {
  return request(window.datacollectorUrl + '/datacollector/cluster/ismaster', {
    method: 'get'
  });
}

export async function getRunnersByTagOrMachineUrl(params) {
  return request(window.datacollectorUrl + '/datacollector/cluster/runners?tag=' + params.tag + '&url=' + params.url, {
    method: 'get'
  });
}