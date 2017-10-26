import wx
from csvdb import csvmemory
from csvdb import csvfile
from csvdb import csvdb
from actions import utils

H_SPACER = 5
V_SPACER = 3

# Make sure these numbers match the indices below 
INNER_JOIN = 0
LEFT_JOIN = 1
RIGHT_JOIN = 2

JOIN_NAMES = ['Inner','Outer Left','Outer Right']

class JoinDialog(wx.Dialog):

  def __init__(self,parent,table):
    wx.Dialog.__init__(self,parent)
    self.table = table
    self.path = None
    self.other_path = None
    self.join_type = INNER_JOIN
    self.join_column = None

    self.other_path_ctrl = None
    self.join_type_ctrl = None
    self.join_column_ctrl = None

    self.initUI()
    self.SetSize((320,240))
    self.SetTitle("Join")


  def setPath(self,v):
    '''
    Required
    '''
    self.path = v

  def initUI(self):
    vbox = wx.BoxSizer(wx.VERTICAL)

    hbox = wx.BoxSizer(wx.HORIZONTAL)
    x = wx.StaticText(self,wx.ID_ANY,label="Other CSV File")
    hbox.AddSpacer(H_SPACER)
    hbox.Add(x)
    hbox.AddSpacer(H_SPACER)
    self.other_path_ctrl = wx.TextCtrl(self,size=(250,wx.ID_ANY))
    self.other_path_ctrl.SetEditable(True)
    hbox.AddSpacer(H_SPACER)
    hbox.Add(self.other_path_ctrl)
    hbox.AddSpacer(H_SPACER)
    self.other_path_button = wx.Button(self) 
    self.other_path_button.SetLabel('...')
    hbox.Add(self.other_path_button)
    self.other_path_button.Bind(wx.EVT_BUTTON,self.onOtherPath)
    vbox.Add(hbox);
    vbox.AddSpacer(V_SPACER)


    hbox = wx.BoxSizer(wx.HORIZONTAL)
    x = wx.StaticText(self,wx.ID_ANY,label="Join Type")
    hbox.Add(x)
    hbox.AddSpacer(H_SPACER)
    self.join_type_ctrl = wx.Choice(self,choices=JOIN_NAMES)
    self.join_type_ctrl.SetStringSelection(JOIN_NAMES[INNER_JOIN])
    hbox.Add(self.join_type_ctrl)
    vbox.Add(hbox);
    vbox.AddSpacer(V_SPACER)

    hbox = wx.BoxSizer(wx.HORIZONTAL)
    x = wx.StaticText(self,wx.ID_ANY,label="Join Column")
    hbox.Add(x)
    hbox.AddSpacer(H_SPACER)
    self.join_column_ctrl = wx.Choice(self,choices=self.table.header)
    self.join_column_ctrl.SetStringSelection(self.table.header[0])
    hbox.Add(self.join_column_ctrl)
    vbox.Add(hbox);
    vbox.AddSpacer(V_SPACER)

    hbox = wx.BoxSizer(wx.HORIZONTAL)
    self.ok_button = wx.Button(self,wx.ID_OK)
    hbox.AddSpacer(H_SPACER)
    hbox.Add(self.ok_button)
    hbox.AddSpacer(H_SPACER)
    self.cancel_button = wx.Button(self,wx.ID_CANCEL)
    hbox.AddSpacer(H_SPACER)
    hbox.Add(self.cancel_button)
    hbox.AddSpacer(H_SPACER)
    vbox.Add(hbox)

    self.ok_button.Bind(wx.EVT_BUTTON,self.onOK)
    self.cancel_button.Bind(wx.EVT_BUTTON,self.onCancel)

    self.SetSizer(vbox)

  def onOK(self,event):
    self.join_type = self.join_type_ctrl.GetCurrentSelection()
    self.join_column = self.join_column_ctrl.GetCurrentSelection()
    self.other_path = self.other_path_ctrl.GetValue()
    self.EndModal(wx.ID_OK)

  def onCancel(self,event):
    self.EndModal(wx.ID_CANCEL)

  def onOtherPath(self,event):
    dialog = wx.FileDialog(self,'Other File')
    chk = dialog.ShowModal()
    if wx.ID_CANCEL != chk:
        self.other_path_ctrl.SetValue(dialog.GetPath())

  def getOtherPath(self):
    return self.other_path

  def getJoinType(self):
    return self.join_type

  def getJoinColumn(self):
    return self.join_column

def doInnerJoin(table,join_idx,other_table,other_join_idx,memdb):
  table.reset()
  for row in table.getIter():
    other_table.reset()
    value = row[join_idx]
    for other_row in other_table.getIter():
      new_row = row[:]
      try:
        other_value = other_row[other_join_idx] 
        if value == other_value:
          idx = 0
          for col in other_row:
            if idx != other_join_idx:
              new_row.append(col)
            idx += 1
          memdb.appendRow(new_row)
      except csvdb.TableException as ex:
        pass
  return memdb

def doLeftJoin(table,join_idx,other_table,other_join_idx,memdb):
  table.reset()
  for row in table.getIter():
    other_table.reset()
    value = row[join_idx]
    new_row = row[:]
    match = False
    for other_row in other_table.getIter():
      try:
        other_value = other_row[other_join_idx] 
        if value == other_value:
          match = True
          idx = 0
          for col in other_row:
            if idx != other_join_idx:
              new_row.append(col)
            idx += 1
          break
      except csvdb.TableException as ex:
        pass
    if False == match:
      for i in range(len(other_table.getHeader())-1):
        new_row.append('')
    memdb.appendRow(new_row)
  return memdb

def doRightJoin(table,join_idx,other_table,other_join_idx,memdb):
  other_table.reset()
  for other_row in other_table.getIter():
    table.reset()
    other_value = other_row[other_join_idx]
    new_row = list()
    match = False
    for row in table.getIter():
      try:
        value = row[join_idx] 
        if value == other_value:
          match = True
          idx = 0
          for col in row:
            if idx != other_join_idx:
              new_row.append(col)
            idx += 1
          break
      except csvdb.TableException as ex:
        pass
    if False == match:
      for i in range(len(table.getHeader())-1):
        new_row.append('')
    for col in other_row:
      new_row.append(col)
    memdb.appendRow(new_row)
  return memdb

def doFullJoin(table,join_idx,other_table,other_join_idx,memdb):
  table.reset()
  for row in table.getIter():
    other_table.reset()
    value = row[join_idx]
    new_row = row[:]
    match = False
    for other_row in other_table.getIter():
      try:
        other_value = other_row[other_join_idx] 
        if value == other_value:
          match = True
          idx = 0
          for col in other_row:
            if idx != other_join_idx:
              new_row.append(col)
            idx += 1
          break
      except csvdb.TableException as ex:
        pass
    if False == match:
      for i in range(len(other_table.getHeader())-1):
        new_row.append('')
    memdb.appendRow(new_row)

  # Do a right join, only including rows we don't have
  other_table.reset()
  for other_row in other_table.getIter():
    table.reset()
    other_value = other_row[other_join_idx]
    new_row = list()
    match = False
    for row in table.getIter():
      value = row[join_idx] 
      if value == other_value:
        match = True
        break

    # FIXME: If the "left" table is shorter than the "right", this is
    # is broken, the unmatched rows don't have the join column in the
    # right place and the rest of the columns are shifted.
    # Or maybe it is because the join column is last in one table and first
    # in the other.
    if False == match:
      for i in range(len(table.getHeader())-1):
        new_row.append('')
      for col in other_row:
        new_row.append(col)
      memdb.appendRow(new_row)
  return memdb



class JoinPlugin(object):

  def __init__(self,parent_frame):
    self.path = None
    self.parent_frame = parent_frame
 
  def getLabel(self):
    '''
    Required
    '''
    return 'Join'

  def getDescription(self):
    '''
    Required
    '''
    return 'Perform a join operation between this and another .csv file'

  def setPath(self,v):
    self.path = v

  def doAction(self,table):
    '''
    Required
    '''
    if None is table:
      wx.MessageBox('Missing table', 'Info', wx.OK | wx.ICON_INFORMATION)
      return
    dialog = JoinDialog(self.parent_frame,table)
    dialog.SetSize((400,-1))
    chk = dialog.ShowModal()
    if wx.ID_OK == chk:
      other_path = dialog.getOtherPath()
      join_type = dialog.getJoinType()
      join_idx = dialog.getJoinColumn()
      join_label = table.getHeader()[join_idx]
      sfr = csvfile.SingleFileReader(other_path)
      other_table = sfr.load() 
      other_join_idx = other_table.getHeaderIndex(join_label)
      memdb = csvmemory.MemoryWriter()
      new_header = []
      for col in table.getHeader():
        new_header.append(col)
      idx = 0
      for col in other_table.getHeader():
        if idx != other_join_idx:
          new_header.append(col)
        idx += 1
      memdb.setHeader(new_header) 
          
      if -1 == join_idx or -1 == other_join_idx:
        wx.MessageBox("Invalid join column",'Info',wx.OK|wx.ICON_INFORMATION)
        return

      if INNER_JOIN == join_type:
        doInnerJoin(table,join_idx,other_table,other_join_idx,memdb)
      elif LEFT_JOIN == join_type:
        doLeftJoin(table,join_idx,other_table,other_join_idx,memdb)
      elif RIGHT_JOIN == join_type:
        doRightJoin(table,join_idx,other_table,other_join_idx,memdb)
            
      path = utils.getTempFilename()
      memdb.save(path)
      self.parent_frame.addPage(path,delete_on_exit=True)
      other_table.close()

def getPlugin(parent_frame):
  return JoinPlugin(parent_frame)


