#region Generated Code
namespace inRiver.Connectors.Inbound.Extended
{
    #endregion

    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Globalization;
    using System.IO;
    using System.Linq;
    using System.Text;
    using System.Xml;

    using inRiver.Integration.Configuration;
    using inRiver.Integration.Reporting;
    using inRiver.Remoting;
    using inRiver.Remoting.Objects;

    using SmartXLS;

    public class Files : Integration.Import.FileImport
    {
        private const string FileEncodingSettingName = "FileEncoding";
        private const string FileEncodingHasByteOrderMarkSettingName = "FileEncodingHasByteOrderMark";
        private const string CSVSeparatorSettingName = "CSVSeparator";
        private static readonly string[] SupportedFileTypes = new[] {".xml", ".xls", ".xlsx", ".csv"};

        private readonly Validation validation = new Validation();

        private XmlDocument mappingDocument;

        internal XmlDocument MappingDocument
        {
            get
            {
                if (this.mappingDocument != null)
                {
                    return this.mappingDocument;
                }

                this.mappingDocument = new XmlDocument();

                try
                {
                    string mapping = RemoteManager.UtilityService.GetConnector(this.Id).Settings["XML_MAPPING"];

                    // removing BOM if it exists
                    mapping = mapping.Replace("ï»¿", string.Empty);
                    this.mappingDocument.LoadXml(mapping);
                }
                catch (Exception ex)
                {
                    ReportManager.Instance.WriteError(this.Id, "An error occurred when parsing connector mapping. " + ex);
                    this.mappingDocument = null;
                }

                return this.mappingDocument;
            }
        }

        internal bool FileEncodingHasByteOrderMark
        {
            get
            {
                bool fileEncodingHasByteOrderMarkStr;
                try
                {
                    bool.TryParse(ConfigurationManager.Instance.GetSetting(Id, FileEncodingHasByteOrderMarkSettingName), out fileEncodingHasByteOrderMarkStr);
                }
                catch (Exception)
                {
                    fileEncodingHasByteOrderMarkStr = false;
                }
                return fileEncodingHasByteOrderMarkStr;
            }
        }

        internal char CSVSeparator
        {
            get
            {
                string separator;
                try
                {
                    separator = ConfigurationManager.Instance.GetSetting(Id, CSVSeparatorSettingName);
                }
                catch (Exception)
                {
                    return ';';
                }

                if (string.IsNullOrEmpty(separator))
                {
                    return ';';
                }

                if (separator.Length > 1)
                {
                    return ';';
                }


                return Convert.ToChar(separator);
            }
        }

        public override void InitFileImportSettings()
        {
            ConfigurationManager.Instance.SetConnectorSetting(Id, FileEncodingSettingName, "Default");
            ConfigurationManager.Instance.SetConnectorSetting(Id, FileEncodingHasByteOrderMarkSettingName, "false");
            ConfigurationManager.Instance.SetConnectorSetting(Id, CSVSeparatorSettingName, ";");
        }

        public override void ProcessFile(string filePath)
        {
            XmlElement documentElement = this.MappingDocument.DocumentElement;

            if (documentElement == null)
            {
                return;
            }

            string uniqueFieldType = this.GetUniqueFieldType();

            string importtype = documentElement.Attributes["importtype"].InnerText;

            switch (importtype)
            {
                case "XML":
                    this.ProcessXml(filePath, uniqueFieldType);
                    break;
                case "Excel":
                    this.ProcessExcel(filePath, uniqueFieldType);
                    break;
                case "CSV":
                    this.ProcessExcel(filePath, uniqueFieldType);
                    break;
            }
        }

        public override bool ShouldProcessFile(string filePath)
        {
            if (this.MappingDocument == null)
            {
                return false;
            }

            if (this.GetEntityType() == null)
            {
                return false;
            }

            if (this.GetImportType() == string.Empty)
            {
                return false;
            }

            string extension = System.IO.Path.GetExtension(filePath);

            if (!SupportedFileTypes.Contains(extension))
            {
                RenameToErrorFile(filePath);
            }

            switch (this.GetImportType())
            {
                case "XML":
                    if (extension != ".xml")
                    {
                        return false;
                    }

                    break;
                case "Excel":
                    if (extension != ".xls" && extension != ".xlsx")
                    {
                        return false;
                    }

                    break;
                case "CSV":
                    if (extension != ".csv")
                    {
                        return false;
                    }

                    break;
                default:
                    return false;
            }

            return true;
        }

        private static void RenameToErrorFile(string filePath)
        {
            string outputpath = $"{filePath}.error";

            if (File.Exists(outputpath))
            {
                File.Delete(outputpath);
            }

            File.Move(filePath, outputpath);
        }

        public void ReadFile(string filePath, WorkBook workBook)
        {
            var extension = System.IO.Path.GetExtension(filePath);

            if (extension != null)
            {
                switch (extension)
                {
                    case ".csv":
                        workBook.CSVSeparator = this.CSVSeparator;
                        workBook.read(filePath);
                        RangeStyle rs = workBook.getRangeStyle(0, 0, 0, workBook.LastCol + 1);//column must set to max column number will be used in csv parsing
                        rs.CustomFormat = "@";
                        workBook.setRangeStyle(rs, 0, 0, 0xfffff, workBook.LastCol + 1);//row1=0 row2=0xfffff to indicate the whole column
                        var streamReader = new StreamReader(filePath, GetFileEncoding(), FileEncodingHasByteOrderMark, 512);
                        workBook.readCSV(streamReader.BaseStream);
                        streamReader.Dispose();
                        break;
                    case ".xls":
                        workBook.read(filePath);
                        break;
                    case ".xlsx":
                        workBook.readXLSX(filePath);
                        break;
                }
            }
        }

        internal DataTable GetDataTable(string filePath)
        {
            WorkBook workBook = new WorkBook();
            this.ReadFile(filePath, workBook);
            DataTable dt = workBook.ExportDataTable(0, 0, workBook.LastRow + 1, workBook.LastCol + 1, true);
            return dt;
        }

        private bool FileContentIsValid(DataTable dataTable)
        {
            var fileContentIsValid = true;
            foreach (DataRow row in dataTable.Rows)
            {
                foreach (DataColumn col in dataTable.Columns)
                {
                    var cellData = row[col.ColumnName].ToString();
                    if (CellContainsControlCharacter(cellData))
                    {
                        fileContentIsValid = false;
                        const string MessageTemplate = "Column {0} should not contain ascii  control characters!";
                        row[this.validation.ValidationErrorColumnName] = string.Format(MessageTemplate, col.ColumnName);
                    }
                }
            }

            return fileContentIsValid;
        }

        private bool CellContainsControlCharacter(string cellContent)
        {
            // ignore Line Feed ,Carriage Return and tab
            cellContent = cellContent.Replace("\n", string.Empty);
            cellContent = cellContent.Replace("\r", string.Empty);
            cellContent = cellContent.Replace("\t", string.Empty);
            char[] fieldContentArray = cellContent.ToCharArray();
            bool fieldContainsControlChar = fieldContentArray.Any(char.IsControl);
            return fieldContainsControlChar;
        }

        internal void WriteDataTableToFile(DataTable dataTable, string filePath, string prefix)
        {
            if (!string.IsNullOrEmpty(prefix) && !prefix.Contains(".") && !prefix.StartsWith("."))
            {
                prefix = string.Format(".{0}", prefix);
            }

            WorkBook workBook = new WorkBook();
            workBook.ImportDataTable(dataTable, true, 0, 0, -1, dataTable.Columns.Count);

            string extension = System.IO.Path.GetExtension(filePath);
            filePath = filePath + prefix;

            if (extension != null && extension.Equals(".csv"))
            {
                workBook.CSVSeparator = ';';
                workBook.writeCSV(filePath);
            }

            if (extension != null && extension.Equals(".xls"))
            {
                workBook.write(filePath);
            }

            if (extension != null && extension.Equals(".xlsx"))
            {
                workBook.writeXLSX(filePath);
            }
        }

        private void ProcessExcel(string filePath, string uniqueFieldType)
        {
            DataTable excelDataTable = this.GetDataTable(filePath);

            excelDataTable.Columns.Add(this.validation.ValidationErrorColumnName);

            if (!FileContentIsValid(excelDataTable))
            {
                WriteDataTableToFile(excelDataTable, filePath, ".error");
                return;
            }

            int index = 1;
            foreach (DataRow row in excelDataTable.Rows)
            {
                Entity entity;

                if (string.IsNullOrEmpty(uniqueFieldType))
                {
                    entity = Entity.CreateEntity(this.GetEntityType());
                    entity = this.SetEntityValues(entity, row, excelDataTable);

                    try
                    {
                        entity = RemoteManager.DataService.AddEntity(entity);
                        ReportManager.Instance.Write(this.Id, "Added entity " + GetDisplayName(entity));
                    }
                    catch (Exception ex)
                    {
                        string errorMessage = string.Format("An error occurred when adding row {0}. {1}", index, ex);
                        ReportManager.Instance.WriteError(this.Id, errorMessage);
                        row[this.validation.ValidationErrorColumnName] = errorMessage;
                    }

                    index++;
                    continue;
                }

                string uniqueValue = row[this.GetMappedUniqueColumn(uniqueFieldType)].ToString();

                if (string.IsNullOrEmpty(uniqueValue))
                {
                    index++;
                    continue;
                }

                entity = RemoteManager.DataService.GetEntityByUniqueValue(uniqueFieldType, uniqueValue, LoadLevel.DataOnly);

                if (entity == null)
                {
                    // Create a new one.
                    entity = Entity.CreateEntity(this.GetEntityType());
                    entity = this.SetEntityValues(entity, row, excelDataTable);

                    try
                    {
                        entity = RemoteManager.DataService.AddEntity(entity);
                        ReportManager.Instance.Write(this.Id, "Added entity " + GetDisplayName(entity));
                    }
                    catch (Exception ex)
                    {
                        string errorMessage = string.Format("An error occurred when adding row {0}. {1}", index, ex);
                        ReportManager.Instance.WriteError(this.Id, errorMessage);
                        row[this.validation.ValidationErrorColumnName] = errorMessage;
                    }

                    index++;
                    continue;
                }

                entity = this.SetEntityValues(entity, row, excelDataTable);

                try
                {
                    RemoteManager.DataService.UpdateEntity(entity);

                    ReportManager.Instance.Write(this.Id, "Updated entity " + GetDisplayName(entity));
                }
                catch (Exception ex)
                {
                    string errorMessage = string.Format("An error occurred when updating row {0}. {1}", index, ex);
                    ReportManager.Instance.WriteError(this.Id, errorMessage);
                    row[this.validation.ValidationErrorColumnName] = errorMessage;
                }

                index++;
            }

            List<DataRow> toRemove = new List<DataRow>();

            foreach (DataRow row in excelDataTable.Rows)
            {
                if (row[this.validation.ValidationErrorColumnName] == DBNull.Value)
                {
                    toRemove.Add(row);
                }
            }

            foreach (var row in toRemove)
            {
                excelDataTable.Rows.Remove(row);
            }

            if (excelDataTable.Rows.Count > 0)
            {
                WriteDataTableToFile(excelDataTable, filePath, ".error");
            }
        }

        private DataRow CreateDataRowWithValidationError(DataTable errorTable, DataTable excelDataTable, DataRow row, string errorMessage)
        {
            DataRow errorRow = errorTable.NewRow();
            foreach (DataColumn column in excelDataTable.Columns)
            {
                errorRow[column.ColumnName] = row[column.ColumnName];
            }

            errorRow[this.validation.ValidationErrorColumnName] = errorMessage;
            return errorRow;
        }

        private void ProcessXml(string filePath, string uniqueFieldType)
        {
            XmlDocument file = new XmlDocument();
            file.Load(filePath);

            XmlElement documentElement = this.MappingDocument.DocumentElement;
            if (documentElement == null)
            {
                return;
            }

            KeyValuePair<XmlDocument, XmlDocument> resultPair = validation.ValidateInput(file, documentElement, uniqueFieldType);

            XmlDocument workDoc = resultPair.Key;
            XmlDocument badResult = resultPair.Value;

            XmlNodeList xmlNodeList = workDoc.SelectNodes(documentElement.Attributes["xpath"].InnerText);
            if (xmlNodeList == null)
            {
                return;
            }

            foreach (XmlNode node in xmlNodeList)
            {
                Entity entity;

                if (string.IsNullOrEmpty(uniqueFieldType))
                {
                    // Create a new one.
                    entity = Entity.CreateEntity(this.GetEntityType());
                    entity = this.SetEntityValues(entity, node);

                    try
                    {
                        entity = RemoteManager.DataService.AddEntity(entity);

                        ReportManager.Instance.Write(this.Id, "Added entity " + GetDisplayName(entity));
                    }
                    catch (Exception ex)
                    {
                        ReportManager.Instance.WriteError(
                            this.Id,
                            "An error occurred when adding node " + node.OuterXml + ". " + ex);
                    }

                    continue;
                }

                XmlNode selectSingleNode = node.SelectSingleNode(this.GetMappedUniqueColumn(uniqueFieldType));
                if (selectSingleNode == null)
                {
                    continue;
                }

                string uniqueValue = selectSingleNode.InnerText;

                if (string.IsNullOrEmpty(uniqueValue))
                {
                    continue;
                }

                entity = RemoteManager.DataService.GetEntityByUniqueValue(uniqueFieldType, uniqueValue, LoadLevel.DataOnly);

                if (entity == null)
                {
                    // Create a new one.
                    entity = Entity.CreateEntity(this.GetEntityType());
                    entity = this.SetEntityValues(entity, node);

                    try
                    {
                        entity = RemoteManager.DataService.AddEntity(entity);

                        ReportManager.Instance.Write(this.Id, "Added entity " + GetDisplayName(entity));
                    }
                    catch (Exception ex)
                    {
                        ReportManager.Instance.WriteError(this.Id, "An error occurred when adding node " + node.OuterXml + ". " + ex);
                    }

                    continue;
                }

                // Otherwise do an update.
                entity = this.SetEntityValues(entity, node);

                try
                {
                    RemoteManager.DataService.UpdateEntity(entity);

                    ReportManager.Instance.Write(this.Id, "Updated entity " + GetDisplayName(entity));
                }
                catch (Exception ex)
                {
                    ReportManager.Instance.WriteError(this.Id, "An error occurred when updating node " + node.OuterXml + ". " + ex);
                }
            }

            var selectNodes = badResult.SelectNodes(documentElement.Attributes["xpath"].InnerText);
            if (selectNodes != null && selectNodes.Count == 0)
            {
                return;
            }

            File.Delete(filePath);
            badResult.Save(filePath + ".error");
        }

        private string GetMappedUniqueColumn(string uniqueFieldType)
        {
            string result = string.Empty;

            XmlElement documentElement = this.MappingDocument.DocumentElement;
            if (documentElement == null)
            {
                return result;
            }

            XmlNode node = documentElement.SelectSingleNode("//fieldtype[text() = '" + uniqueFieldType + "']");

            if (node == null)
            {
                return result;
            }

            if (node.PreviousSibling != null)
            {
                result = node.PreviousSibling.InnerText;
            }

            return result;
        }

        private string GetLanguageFromColumnName(string columnName)
        {
            string result = string.Empty;

            XmlElement documentElement = this.MappingDocument.DocumentElement;
            if (documentElement == null)
            {
                return result;
            }

            XmlNode node = documentElement.SelectSingleNode("//nodename[text() = '" + columnName + "']");

            if (node == null)
            {
                return result;
            }

            while (node.NextSibling != null)
            {
                node = node.NextSibling;
                if (node.Name != "language")
                {
                    continue;
                }

                result = node.InnerText;
                break;
            }

            return result;
        }

        private string GetFieldTypeFromColumnName(string columnName)
        {
            string result = string.Empty;

            XmlElement documentElement = this.MappingDocument.DocumentElement;
            if (documentElement == null)
            {
                return result;
            }

            XmlNode node = documentElement.SelectSingleNode("//nodename[text() = '" + columnName + "']");

            if (node == null)
            {
                return result;
            }

            if (node.NextSibling != null)
            {
                result = node.NextSibling.InnerText;
            }

            return result;
        }

        private Entity SetEntityValues(Entity entity, XmlNode node)
        {
            foreach (XmlNode fieldNode in node.ChildNodes)
            {
                string fieldTypeId = this.GetFieldTypeFromColumnName(fieldNode.Name);
                string language = this.GetLanguageFromColumnName(fieldNode.Name);

                if (string.IsNullOrEmpty(fieldTypeId))
                {
                    continue;
                }

                FieldType fieldType = entity.EntityType.FieldTypes.FirstOrDefault(ft => ft.Id == fieldTypeId);

                if (fieldType == null)
                {
                    continue;
                }

                entity.GetField(fieldTypeId).Data = this.GetFieldData(fieldNode.InnerText, language, fieldType, entity.GetField(fieldTypeId));
            }

            return entity;
        }

        private Entity SetEntityValues(Entity entity, DataRow row, DataTable table)
        {
            foreach (DataColumn col in table.Columns)
            {
                if (col.ColumnName == "sys_id" || string.IsNullOrEmpty(col.ColumnName))
                {
                    continue;
                }

                string fieldTypeId = this.GetFieldTypeFromColumnName(col.ColumnName);
                string language = this.GetLanguageFromColumnName(col.ColumnName);

                if (string.IsNullOrEmpty(fieldTypeId))
                {
                    continue;
                }

                FieldType fieldType = RemoteManager.ModelService.GetFieldType(fieldTypeId);
                entity.GetField(fieldTypeId).Data = this.GetFieldData(row[col.ColumnName].ToString(), language, fieldType, entity.GetField(fieldTypeId));
            }

            return entity;
        }

        private object GetFieldData(string content, string language, FieldType fieldType, Field referenceField)
        {
            if (string.IsNullOrEmpty(content) && fieldType.DataType != "LocaleString")
            {
                return null;
            }

            switch (fieldType.DataType)
            {
                case "Boolean":
                    bool resultBoolean;
                    bool.TryParse(content, out resultBoolean);
                    return resultBoolean;

                case "Double":
                    double resultDouble;
                    double.TryParse(content.Replace(",", "."), NumberStyles.AllowDecimalPoint, CultureInfo.CreateSpecificCulture("en"), out resultDouble);
                    return resultDouble;
                case "File":
                case "Integer":
                    int resultInteger;
                    int.TryParse(content, out resultInteger);
                    return resultInteger;
                case "DateTime":
                    DateTime resultDateTime;
                    DateTime.TryParse(content, out resultDateTime);
                    double value;
                    if (double.TryParse(content, out value))
                    {
                        resultDateTime = DateTime.FromOADate(value);
                    }

                    return resultDateTime;
                case "CVL":
                    return content;
                case "LocaleString":

                    if (string.IsNullOrEmpty(language))
                    {
                        return referenceField.Data;
                    }

                    LocaleString resultLocaleString = (LocaleString)referenceField.Data;
                    if (resultLocaleString == null)
                    {
                        resultLocaleString = new LocaleString(RemoteManager.UtilityService.GetAllLanguages());
                    }

                    CultureInfo ci = new CultureInfo(language);
                    if (ci.Name == language)
                    {
                        resultLocaleString[ci] = content;
                    }

                    return resultLocaleString;
                default:
                    if (string.IsNullOrEmpty(content))
                    {
                        return string.Empty;
                    }

                    if (content.ToLower() == "guid")
                    {
                        return Guid.NewGuid().ToString();
                    }

                    return content;
            }
        }

        private string GetUniqueFieldType()
        {
            string result = string.Empty;
            EntityType entityType = this.GetEntityType();

            XmlElement documentElement = this.MappingDocument.DocumentElement;
            if (documentElement == null)
            {
                return result;
            }

            XmlNodeList xmlNodeList = documentElement.SelectNodes("//fieldtype");
            if (xmlNodeList == null)
            {
                return result;
            }

            foreach (XmlNode node in xmlNodeList)
            {
                if (string.IsNullOrEmpty(node.InnerText))
                {
                    continue;
                }

                FieldType fieldType = entityType.FieldTypes.FirstOrDefault(ft => string.Equals(ft.Id, node.InnerText, StringComparison.CurrentCultureIgnoreCase));

                if (fieldType == null)
                {
                    continue;
                }

                if (fieldType.Unique)
                {
                    result = fieldType.Id;
                    break;
                }
            }

            return result;
        }

        private EntityType GetEntityType()
        {
            XmlElement documentElement = this.MappingDocument.DocumentElement;
            if (documentElement == null)
            {
                return null;
            }

            string entityTypeId = documentElement.Attributes["entitytype"].InnerText;

            if (string.IsNullOrEmpty(entityTypeId))
            {
                return null;
            }

            return RemoteManager.ModelService.GetEntityType(entityTypeId);
        }

        private string GetImportType()
        {
            string result = string.Empty;

            XmlElement documentElement = this.MappingDocument.DocumentElement;
            if (documentElement != null)
            {
                result = documentElement.Attributes["importtype"].InnerText;
            }

            return result;
        }

        private string GetDisplayName(Entity entity)
        {
            if (entity != null)
            {
                var displayname = entity.DisplayName == null ? entity.Id : entity.DisplayName.Data;
                return displayname.ToString();
            }
            return string.Empty;
        }

        private Encoding GetFileEncoding()
        {
            Encoding encoding;
            try
            {
                var encodingName = ConfigurationManager.Instance.GetSetting(Id, FileEncodingSettingName);
                encoding = Encoding.GetEncoding(encodingName);
            }
            catch (Exception)
            {
                encoding = Encoding.Default;
            }

            return encoding;
        }
    }
}
