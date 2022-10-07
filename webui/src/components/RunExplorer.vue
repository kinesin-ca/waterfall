<script>
import RunList from './RunList.vue';
import RunDetails from './RunDetails.vue';

export default {
  props: ['refreshSeconds', 'daggydURL'],
  data() {
    return {
      activeRunID: null,
      activeTaskName: null,
    };
  },
  components: {
    RunList,
    RunDetails,
  },
  methods: {
    setActiveRunID(runID) {
      this.activeRunID = runID;
    },
  },
};
</script>

<template>
  <div id="explorer">
    <RunList
      :daggydURL="daggydURL"
      :refreshSeconds="refreshSeconds"
      @update-active-runid="(runID) => setActiveRunID(runID)"
    />
    <RunDetails
      :daggydURL="daggydURL"
      :refreshSeconds="refreshSeconds"
      :activeRunID="activeRunID"
      />
  </div>
</template>
