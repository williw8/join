import wx
from csvdb import csvmemory
from csvdb import csvfile
from csvdb import csvdb
from actions import utils

BOX_SPACER = 5

# Make sure these numbers match the indices below 
INNER_JOIN = 0
LEFT_JOIN = 1
RIGHT_JOIN = 2
FULL_JOIN = 3

JOIN_NAMES = ['Inner','Outer Left','Outer Right','Outer Full']

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
    x = wx.StaticText(self,-1,"Other CSV File")
    hbox.AddSpacer(BOX_SPACER)
    hbox.Add(x)
    hbox.AddSpacer(BOX_SPACER)
    self.other_path_ctrl = wx.TextCtrl(self,size=(250,-1))
    self.other_path_ctrl.SetEditable(True)
    hbox.AddSpacer(BOX_SPACER)
    hbox.Add(self.other_path_ctrl)
    hbox.AddSpacer(BOX_SPACER)
    self.other_path_button = wx.Button(self) 
    self.other_path_button.SetLabel('...')
    hbox.Add(self.other_path_button)
    self.other_path_button.Bind(wx.EVT_BUTTON,self.onOtherPath)
    vbox.Add(hbox);


    hbox = wx.BoxSizer(wx.HORIZONTAL)
    x = wx.StaticText(self,-1,"Join Type")
    hbox.Add(x)
    self.join_type_ctrl = wx.ComboBox(self,style=wx.CB_DROPDOWN,choices=JOIN_NAMES)
    self.join_type_ctrl.SetEditable(False)
    self.join_type_ctrl.SetStringSelection(JOIN_NAMES[INNER_JOIN])
    hbox.Add(self.join_type_ctrl)
    vbox.Add(hbox);

    hbox = wx.BoxSizer(wx.HORIZONTAL)
    x = wx.StaticText(self,-1,"Join Column")
    hbox.Add(x)
    self.join_column_ctrl = wx.ComboBox(self,style=wx.CB_DROPDOWN,choices=self.table.header)
    self.join_column_ctrl.SetEditable(False)
    self.join_column_ctrl.SetStringSelection(self.table.header[0])
    hbox.Add(self.join_column_ctrl)
    vbox.Add(hbox);

    hbox = wx.BoxSizer(wx.HORIZONTAL)
    self.ok_button = wx.Button(self,wx.ID_OK)
    hbox.AddSpacer(BOX_SPACER)
    hbox.Add(self.ok_button)
    hbox.AddSpacer(BOX_SPACER)
    self.cancel_button = wx.Button(self,wx.ID_CANCEL)
    hbox.AddSpacer(BOX_SPACER)
    hbox.Add(self.cancel_button)
    hbox.AddSpacer(BOX_SPACER)
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
      memw = csvmemory.MemoryWriter()
      new_header = []
      for col in table.getHeader():
        new_header.append(col)
      idx = 0
      for col in other_table.getHeader():
        if idx != other_join_idx:
          new_header.append(col)
        idx += 1
      memw.setHeader(new_header) 
          
      if INNER_JOIN == join_type:
        if -1 == join_idx or -1 == other_join_idx:
          wx.MessageBox("Invalid join column",'Info',wx.OK|wx.ICON_INFORMATION)
          return
        table.reset()
        for row in table.getIter():
          other_table.reset()
          value = row[join_idx]
          new_row = row

          append = False
          for other_row in other_table.getIter():
            try:
              other_value = other_row[other_join_idx] 
              if value == other_value:
                append = True
                idx = 0
                for col in other_row:
                  if idx != other_join_idx:
                    new_row.append(col)
                  idx += 1
                break
            except csvdb.TableException as ex:
              pass
          if append:
            memw.appendRow(new_row)
        
      path = utils.getTempFilename()
      memw.save(path)
      self.parent_frame.addPage(path,delete_on_exit=True)
      other_table.close()

def getPlugin(parent_frame):
  return JoinPlugin(parent_frame)


