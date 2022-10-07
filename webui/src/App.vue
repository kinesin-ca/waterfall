<script>
import RunExplorer from './components/RunExplorer.vue'
import GlobalSettings from './components/GlobalSettings.vue'

export default {
  data() {
    return {
      refreshSeconds: 15,  // How often to refresh
      daggydURL: window.location.origin,
    }
  },

  methods: {
    updateURL(url) {
      this.daggydURL = url;
    },
    updateRefreshInterval(interval) {
      this.refreshSeconds = interval;
    },
  },

  components: {
    GlobalSettings,
    RunExplorer
  }
}
</script>

<style>
select { max-width: 25%; }
input { max-width: 25%; }
</style>

<template>
  <div id="settings">
    <GlobalSettings
      :daggydURL="daggydURL"
      :refreshSeconds="refreshSeconds"
      @update-refresh-interval="(interval) => this.updateRefreshInterval(interval)"
      @update-daggyd-url="(url) => this.updateURL(url)"
      />
  </div>
  <div id="explorer">
    <RunExplorer
      :refreshSeconds="refreshSeconds"
      :daggydURL="daggydURL"
      @new-active-run="(runID) => this.activeRunID = runID"
    />
  </div>
</template>
