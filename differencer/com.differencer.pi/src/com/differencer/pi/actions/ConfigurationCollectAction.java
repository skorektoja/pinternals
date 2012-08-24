package com.differencer.pi.actions;
import java.io.IOException;
import java.util.Iterator;
import java.util.Vector;
import javax.xml.parsers.ParserConfigurationException;
import org.eclipse.core.resources.IFile;
import org.eclipse.core.runtime.CoreException;
import org.eclipse.core.runtime.Status;
import org.eclipse.jface.action.IAction;
import org.eclipse.jface.dialogs.MessageDialog;
import org.eclipse.jface.viewers.ISelection;
import org.eclipse.jface.viewers.StructuredSelection;
import org.eclipse.ui.IObjectActionDelegate;
import org.eclipse.ui.IWorkbenchPart;
import org.eclipse.ui.PlatformUI;
import org.xml.sax.SAXException;
import com.differencer.pi.Activator;
import com.differencer.pi.Differencer;
import com.differencer.pi.editors.Server;
import com.differencer.pi.editors.ServerConfigurationJob;
public class ConfigurationCollectAction implements IObjectActionDelegate {
	private Vector<Server> descriptions = new Vector<Server>();
	public ConfigurationCollectAction() {
	}
	@Override
	public void setActivePart(IAction action, IWorkbenchPart targetPart) {
	}
	@Override
	public void run(IAction action) {
		Iterator<Server> iterator = descriptions.iterator();
		while (iterator.hasNext()) {
			startCollectionJob(iterator.next());
		}
	}
	public static void startCollectionJob(Server description) {
		if (Differencer.isConnectedDatabase()) {
			MessageDialog dialog = new MessageDialog(PlatformUI.getWorkbench().getActiveWorkbenchWindow().getShell(), "Data collection type", null, "Collect bulk?", MessageDialog.QUESTION, new String[] { "Yes", "No", "Cancel" }, 2);
			int result = dialog.open();
			if (result < 2) {
				ServerConfigurationJob job = new ServerConfigurationJob("Collect PI configuration from host " + description.getURL(), description, result == 0 ? true : false);
				job.schedule();
			}
		} else {
			MessageDialog.openInformation(PlatformUI.getWorkbench().getActiveWorkbenchWindow().getShell(), "Database connection status", "Database disconnected");
		}
	}
	@Override
	public void selectionChanged(IAction action, ISelection selection) {
		descriptions.clear();
		Iterator<IFile> iterator = ((StructuredSelection) selection).iterator();
		while (iterator.hasNext()) {
			try {
				descriptions.add(new Server(iterator.next()));
			} catch (ParserConfigurationException e) {
				Activator.log(Status.ERROR, getClass().getName(), e);
			} catch (SAXException e) {
				Activator.log(Status.ERROR, getClass().getName(), e);
			} catch (IOException e) {
				Activator.log(Status.ERROR, getClass().getName(), e);
			} catch (CoreException e) {
				Activator.log(Status.ERROR, getClass().getName(), e);
			}// no PI files are filtered out by Eclipse :)
		}
	}
}
