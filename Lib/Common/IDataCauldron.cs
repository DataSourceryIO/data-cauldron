namespace DataSourcery.Data
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;

    public interface IDatacauldron
    {
        Dictionary<String, IDataSource> Registry { get; }

        Task RegisterDataSources(Type dataSourceType, string dataSoureTypeConfigurationFileNamePattern);

        IDatacauldron SetConfigurationContainer(object container);

        IDatacauldron SetExtractContainer(object container);
    }
}