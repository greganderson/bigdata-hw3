import xml.etree.ElementTree as ET

f = 'small_pages/page-0001000.xml'
tree = ET.parse(f)
root = tree.getroot()
text = root[3][7]

# Turn every abnormal character into a space?
