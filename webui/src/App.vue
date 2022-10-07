<script>
import Timeline from './components/Timeline.vue'
import GlobalSettings from './components/GlobalSettings.vue'
import SegmentDetails from './components/SegmentDetails.vue'

export default {
  data() {
    return {
      refreshSeconds: 15,  // How often to refresh
      waterfallURL: 'http://localhost:2503',
      activeSegment: null,
    }
  },

  methods: {
    updateURL(url) {
      this.waterfallURL = url;
    },
    updateRefreshInterval(interval) {
      this.refreshSeconds = interval;
    },
    setActiveSegment(segment) {
      this.activeSegment = segment;
    },
  },

  components: {
    GlobalSettings,
    Timeline,
    SegmentDetails,
  },
};
</script>

<style>
select { max-width: 25%; }
input { max-width: 25%; }
</style>

<template>
    <GlobalSettings
      :waterfallURL="waterfallURL"
      :refreshSeconds="refreshSeconds"
      @update-refresh-interval="(interval) => this.updateRefreshInterval(interval)"
      @update-waterfall-url="(url) => this.updateURL(url)"
      />
  <br/>
  <div>
    <Timeline
      :waterfallURL="waterfallURL"
      :refreshSeconds="refreshSeconds"
      @update-active-segment="(segment) => this.setActiveSegment(segment)"
    />
  </div>
  <br/>
  <SegmentDetails
    v-if="this.activeSegment !== null"
    :activeSegment="activeSegment"
    />
</template>
