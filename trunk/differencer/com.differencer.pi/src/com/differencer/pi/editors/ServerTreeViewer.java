package com.differencer.pi.editors;
import java.util.HashMap;
import java.util.Iterator;
import org.eclipse.core.runtime.Platform;
import org.eclipse.core.runtime.Status;
import org.eclipse.core.runtime.preferences.IPreferencesService;
import org.eclipse.jface.action.ActionContributionItem;
import org.eclipse.jface.action.IMenuListener;
import org.eclipse.jface.action.IMenuManager;
import org.eclipse.jface.action.MenuManager;
import org.eclipse.jface.dialogs.MessageDialog;
import org.eclipse.jface.viewers.DoubleClickEvent;
import org.eclipse.jface.viewers.IDoubleClickListener;
import org.eclipse.jface.viewers.ISelection;
import org.eclipse.jface.viewers.IStructuredSelection;
import org.eclipse.jface.viewers.TreeSelection;
import org.eclipse.jface.viewers.TreeViewer;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.ui.IEditorDescriptor;
import org.eclipse.ui.IStorageEditorInput;
import org.eclipse.ui.IWorkbenchPage;
import org.eclipse.ui.PartInitException;
import org.eclipse.ui.PlatformUI;
import com.differencer.pi.Activator;
import com.differencer.pi.actions.ExportToClientTransportAction;
import com.differencer.pi.nodes.ConfigurationNode;
import com.differencer.pi.preferences.PreferenceConstants;
public class ServerTreeViewer extends TreeViewer implements IDoubleClickListener {
	public ServerTreeViewer(Composite parent) {
		super(parent);
		addDoubleClickListener(this);
		MenuManager menuMgr = new MenuManager("#QuoteItemTree"); //$NON-NLS-1$
		menuMgr.setRemoveAllWhenShown(true);
		menuMgr.addMenuListener(new IMenuListener() {
			@Override
			public void menuAboutToShow(IMenuManager manager) {
				boolean enable = false;
				ISelection selection = getSelection();
				HashMap<ConfigurationNode, ConfigurationNode> nodes = new HashMap<ConfigurationNode, ConfigurationNode>();
				if (selection instanceof IStructuredSelection) {
					Iterator<IStructuredSelection> elements = ((IStructuredSelection) selection).iterator();
					while (elements.hasNext()) {
						Object element = elements.next();
						if (element instanceof ConfigurationNode) {
							enable = true;
							ConfigurationNode leaf = (ConfigurationNode) element;
							if (leaf.getChildren().length != 0) {
								enable = true;
								Object[] children = leaf.getChildren();
								for (int i = 0; i < children.length; i++) {
									ConfigurationNode child = (ConfigurationNode) children[i];
									nodes.put(child, child);
								}
							}
							nodes.put(leaf, leaf);
						}
					}
				}
				IPreferencesService service = Platform.getPreferencesService();
				String transportDirectory = service.getString(Activator.PLUGIN_ID, PreferenceConstants.P_TRANSPORT_PATH, "not found transport directory preference!", null);
				String transportArchiveDirectory = service.getString(Activator.PLUGIN_ID, PreferenceConstants.P_TRANSPORT_ARCHIVE_PATH, "not found transport archive directory preference!", null);
				ExportToClientTransportAction exportToClientTransportAction = new ExportToClientTransportAction(nodes, transportDirectory, transportArchiveDirectory);
				ActionContributionItem actionToClientContributionItem = new ActionContributionItem(exportToClientTransportAction);
				actionToClientContributionItem.setVisible(true);
				exportToClientTransportAction.setEnabled(enable);
				manager.add(actionToClientContributionItem);
			}
		});
		getControl().setMenu(menuMgr.createContextMenu(getControl()));
	}
	@Override
	public void doubleClick(DoubleClickEvent event) {
		Object element = ((TreeSelection) event.getSelection()).getFirstElement();
		if (!(element instanceof ConfigurationNode)) return;
		ConfigurationNode leaf = (ConfigurationNode) element;
		IWorkbenchPage page = PlatformUI.getWorkbench().getActiveWorkbenchWindow().getActivePage();
		IEditorDescriptor desc = PlatformUI.getWorkbench().getEditorRegistry().getDefaultEditor(leaf.getName() + "." + leaf.getType());
		try {
			IStorageEditorInput iStorageEditorInput = null;
			if (((ConfigurationNode) element).getNode().getPayload() == null) {
				if (MessageDialog.openConfirm(PlatformUI.getWorkbench().getActiveWorkbenchWindow().getShell(), "Recoursive payload", "Get payload from all nodes")) {
					iStorageEditorInput = new ServerStorageEditorInput(new ServerStorage((ConfigurationNode) element));
					page.openEditor(iStorageEditorInput, desc.getId());
				}
			} else {
				iStorageEditorInput = new ServerStorageEditorInput((ConfigurationNode) element);
				page.openEditor(iStorageEditorInput, desc.getId());
			}
			// TODO open XML editor here
			// page.openEditor(new ServerStorageEditorInput((ConfigurationNode)
			// element),
			// "org.eclipse.wst.xml.ui.internal.tabletree.XMLMultiPageEditorPart");
		} catch (PartInitException e) {
			Activator.log(Status.ERROR, getClass().getName(), e);
		}
	}
}
