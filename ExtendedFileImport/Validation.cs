#region Generated Code
namespace inRiver.Connectors.Inbound.Extended
{
#endregion

    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Globalization;
    using System.Linq;
    using System.Xml;

    using inRiver.Remoting;
    using inRiver.Remoting.Objects;

    public class Validation
    {
        private const string ErrorColumnName = "Validation Error";

        public string ValidationErrorColumnName
        {
            get
            {
                return ErrorColumnName;
            }
        }

        public KeyValuePair<DataTable, DataTable> ValidateInput(DataTable dataTable, XmlElement mapping, string uniqueFieldType)
        {
            DataTable badContent = dataTable.Copy();

            DataTable goodContent = dataTable.Copy();

            badContent.Columns.Add(ValidationErrorColumnName, typeof(string));

            IList<KeyValuePair<string, string>> existingMappings = GetExistingMappings(mapping);

            IList<int> goodRowList = new List<int>();
            int rowNr = -1;
            foreach (DataRow row in badContent.Rows)
            {
                rowNr++;
                bool error = false;
                string errorText = string.Empty;

                if (!string.IsNullOrEmpty(row.RowError))
                {
                    errorText = string.Format("{0}Row Error: {1}. ", errorText, row.RowError);
                    error = true;
                }

                int foundColumns = 0;
                if (!error)
                {
                    foreach (DataColumn column in badContent.Columns)
                    {
                        if (column.ColumnName == ValidationErrorColumnName)
                        {
                            break;
                        }

                        if (existingMappings.Count(p => p.Key == column.ColumnName) == 0)
                        {
                            // Do not need to validate as it's not in scope.
                            continue;
                        }

                        foundColumns++;

                        string value = row[column.ColumnName].ToString();
                        string fieldTypeId = existingMappings.First(p => p.Key == column.ColumnName).Value;

                        FieldType fieldType = RemoteManager.ModelService.GetFieldType(fieldTypeId);

                        if (fieldType == null)
                        {
                            errorText = string.Format("{0}Column {1}: Mapped field type does not exist. ", errorText, column.ColumnName);
                            error = true;

                            continue;
                        }

                        if (string.IsNullOrEmpty(value))
                        {
                            if (fieldType.Mandatory)
                            {
                                errorText = string.Format("{0}Column {1}: Field is mandatory but no value was provided. ", errorText, column.ColumnName);
                                error = true;
                            }

                            break;
                        }

                        if (ValidateValueAgainstFieldType(fieldType, value))
                        {
                            continue;
                        }

                        errorText = string.Format("{0}Column {1}: The value does not match field type ({2}) data type. ", errorText, column.ColumnName, fieldType.DataType);
                        error = true;
                    }
                }

                if (!error && foundColumns > 0)
                {
                    goodRowList.Add(rowNr);
                    continue;
                }

                if (foundColumns == 0)
                {
                    errorText = string.Format("{0}General: No column match to the mapping setup. Will not continue! ", errorText);
                    row[ValidationErrorColumnName] = errorText;
                    break;
                }

                row[ValidationErrorColumnName] = errorText;
            }

            // Clean out the good rows from the result.
            List<DataRow> goodRows = new List<DataRow>();
            foreach (int nr in goodRowList)
            {
                goodRows.Add(badContent.Rows[nr]);
                goodContent.Rows.Add(dataTable.Rows[nr]);
            }

            foreach (DataRow goodRow in goodRows)
            {
                badContent.Rows.Remove(goodRow);
            }

            // return the good and bad result...
            return new KeyValuePair<DataTable, DataTable>(goodContent, badContent);
        }

        public KeyValuePair<XmlDocument, XmlDocument> ValidateInput(XmlDocument document, XmlElement mapping, string uniqueFieldType)
        {
            string workingXpath = mapping.Attributes["xpath"].Value;
            string[] split = workingXpath.Split(new[] { "/" }, StringSplitOptions.RemoveEmptyEntries);
            XmlNodeList nodeList = document.GetElementsByTagName(split[split.Count() - 1]);

            IList<KeyValuePair<string, string>> existingMappings = this.GetExistingMappings(mapping);

            KeyValuePair<List<XmlNode>, List<XmlNode>> resultPair = this.ValidateXmlMappings(nodeList, existingMappings);

            XmlDocument goodDocument = new XmlDocument();
            if (resultPair.Key.Count > 0)
            {
                goodDocument = this.ReplaceDocument(document, resultPair.Key);
            }

            XmlDocument badDocument = new XmlDocument();
            if (resultPair.Value.Count > 0)
            {
                badDocument = this.ReplaceDocument(document, resultPair.Value);
            }

            return new KeyValuePair<XmlDocument, XmlDocument>(goodDocument, badDocument);
        }

        private XmlDocument ReplaceDocument(XmlDocument document, List<XmlNode> replacementNodes)
        {
            XmlDocument result = new XmlDocument();

            if (!document.HasChildNodes)
            {
                return result;
            }

            XmlNodeList nodes = document.ChildNodes;
            foreach (XmlNode node in nodes)
            {
                XmlNode copied;
                if (!node.HasChildNodes)
                {
                    if (node.NodeType != XmlNodeType.XmlDeclaration)
                    {
                        copied = result.ImportNode(node, false);
                        result.AppendChild(copied);
                    }

                    continue;
                }

                if (replacementNodes.Count(p => p.Name == node.Name) > 0)
                {
                    copied = result.ImportNode(node, false);
                    result.AppendChild(copied);
                    continue;
                }

                copied = result.ImportNode(node, false);
                XmlNode appended = result.AppendChild(copied);
                result.ReplaceChild(this.GetChildNode(node.ChildNodes, appended, result), appended);
            }

            return result;
        }

        private XmlNode GetChildNode(XmlNodeList nodes, XmlNode parentNode, XmlDocument referenceDocument)
        {
            // Recursive...
            foreach (XmlNode node in nodes)
            {
                XmlNode copied;
                if (!node.HasChildNodes)
                {
                    copied = referenceDocument.ImportNode(node, false);
                    parentNode.AppendChild(copied);
                    continue;
                }

                copied = referenceDocument.ImportNode(node, false);
                XmlNode appended = parentNode.AppendChild(copied);
                parentNode.ReplaceChild(this.GetChildNode(node.ChildNodes, appended, referenceDocument), appended);
            }

            return parentNode;
        }

        private KeyValuePair<List<XmlNode>, List<XmlNode>> ValidateXmlMappings(XmlNodeList nodeList, IList<KeyValuePair<string, string>> existingMappings)
        {
            List<XmlNode> goodResult = new List<XmlNode>();
            List<XmlNode> badResult = new List<XmlNode>();

            int foundConnections = 0;
            foreach (XmlNode node in nodeList)
            {
                if (!node.HasChildNodes)
                {
                    badResult.Add(node);
                    continue;
                }

                bool error = false;
                foreach (XmlNode childNode in node.ChildNodes)
                {
                    int count = existingMappings.Count(p => p.Key == childNode.Name);
                    if (count == 0)
                    {
                        continue;
                    }

                    foundConnections++;

                    string value = childNode.InnerText;

                    // pair.Key = extern parameter
                    // pair.Value = inRiver parameter
                    KeyValuePair<string, string> pair = existingMappings.First(p => p.Key == childNode.Name);

                    FieldType fieldType = RemoteManager.ModelService.GetFieldType(pair.Value);
                    if (fieldType == null)
                    {
                        error = true;
                        break;
                    }

                    if (string.IsNullOrEmpty(value))
                    {
                        if (fieldType.Mandatory)
                        {
                            error = true;
                        }

                        break;
                    }

                    switch (fieldType.DataType)
                    {
                        case "Boolean":
                            bool resultBoolean;
                            error = !bool.TryParse(value, out resultBoolean);
                            break;
                        case "Double":
                            double resultDouble;
                            error =
                                !double.TryParse(
                                    value.Replace(",", "."),
                                    NumberStyles.AllowDecimalPoint,
                                    CultureInfo.CreateSpecificCulture("en"),
                                    out resultDouble);
                            break;
                        case "Integer":
                            int resultInteger;
                            error =
                                !int.TryParse(
                                    value.Replace(",", "."),
                                    NumberStyles.AllowDecimalPoint,
                                    CultureInfo.CreateSpecificCulture("en"),
                                    out resultInteger);
                            break;
                        case "DateTime":
                            DateTime resultDateTime;
                            error = !DateTime.TryParse(value, out resultDateTime);
                            break;
                        case "CVL":
                            foreach (var key in value.Split(';'))
                            {
                                var cvlValue = RemoteManager.ModelService.GetCVLValueByKey(key, fieldType.CVLId);

                                if (cvlValue == null)
                                {
                                    error = true;
                                }
                            }

                            break;
                    }

                    if (error)
                    {
                        break;
                    }
                }

                if (!error && foundConnections > 0)
                {
                    goodResult.Add(node);
                    continue;
                }

                if (foundConnections == 0)
                {
                    badResult.Add(node);
                    break;
                }

                badResult.Add(node);
            }

            if (foundConnections > 0)
            {
                return new KeyValuePair<List<XmlNode>, List<XmlNode>>(goodResult, badResult);
            }

            goodResult = new List<XmlNode>();
            badResult = new List<XmlNode>();
            foreach (XmlNode node in nodeList)
            {
                badResult.Add(node);
            }

            return new KeyValuePair<List<XmlNode>, List<XmlNode>>(goodResult, badResult);
        }

        private IList<KeyValuePair<string, string>> GetExistingMappings(XmlElement mapping)
        {
            IList<KeyValuePair<string, string>> existingMappings = new List<KeyValuePair<string, string>>();
            XmlNodeList mappingNodeList = mapping.GetElementsByTagName("mapping");
            foreach (XmlNode node in mappingNodeList)
            {
                if (!node.HasChildNodes && node.ChildNodes.Count < 2)
                {
                    continue;
                }

                if (string.IsNullOrEmpty(node.FirstChild.InnerText) || string.IsNullOrEmpty(node.ChildNodes[1].InnerText))
                {
                    continue;
                }

                if (existingMappings.Count(p => p.Key == node.FirstChild.InnerText) > 0)
                {
                    continue;
                }

                existingMappings.Add(new KeyValuePair<string, string>(node.FirstChild.InnerText, node.ChildNodes[1].InnerText));
            }

            return existingMappings;
        }

        private bool ValidateValueAgainstFieldType(FieldType fieldType, string value)
        {
            switch (fieldType.DataType)
            {
                case "Boolean":
                    bool resultBoolean;
                    return bool.TryParse(value, out resultBoolean);

                case "Double":
                    double resultDouble;
                    return double.TryParse(value.Replace(",", "."), NumberStyles.AllowDecimalPoint, CultureInfo.CreateSpecificCulture("en"), out resultDouble);

                case "Integer":
                    int resultInteger;
                    return int.TryParse(value, out resultInteger);

                case "DateTime":
                    DateTime resultDateTime;
                    return DateTime.TryParse(value, out resultDateTime);

                case "CVL":
                    return RemoteManager.ModelService.GetCVLValueByKey(value, fieldType.CVLId) != null;

                default:
                    return true;
            }
        }
    }
}