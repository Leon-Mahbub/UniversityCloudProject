using Microsoft.Azure.Cosmos.Table;
using System;
using System.Collections.Generic;
using System.Text;

namespace MyCloudProject.Common
{
    public class ExperimentResult : TableEntity
    {
        public ExperimentResult(string partitionKey, string rowKey)
        {
            this.PartitionKey = partitionKey;
            this.ExperimentId = rowKey;
        }

        public string ExperimentId { get; set; }

        public string Name { get; set; }

        public string Description { get; set; }

        public DateTime StartTimeUtc { get; set; }

        public DateTime EndTimeUtc { get; set; }

        public long DurationSec { get; set; }

        public string InputFileUrl { get; set; }

        public string[] OutputFiles { get; set; }
        // Your properties related to experiment.

        public float Accuracy { get; set; }

        //public float ?? {get;set

    }
}
