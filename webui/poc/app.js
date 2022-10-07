function renderDetails(segment) {
    document.getElementById("details").innerHTML = JSON.stringify(segment);
}

fetch("http://localhost:2503/api/v1/details",
  {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json'
    },
    body: '{ "start": "2021-09-01T00:00:00Z", "end": "2022-10-01T00:00:00Z" }'
  }
)
  .then((response) =>  {
    if (!response.ok) {
      throw new Error('Network response was not OK');
    }
    return response.json();
  })
  .then((payload) => {
    console.log(payload);
    payload.map((group) => {
      Object.values(group.data).map((label) => {
        label.data.map((interval) => {
          interval.timeRange = interval.timeRange.map((t) => new Date(t));
        })
      })
    });
    TimelinesChart()
    (document.getElementById("timeline"))
      .timeFormat("%Y-%m-%dT%H:%M:%S.%LZ")    // ISO 8601 format
      .zScaleLabel('State')
      .zQualitative(true)
      .useUtc(false)
      .onSegmentClick(renderDetails)
      .data(payload)
  }
  )
  .catch(err => { throw err });
